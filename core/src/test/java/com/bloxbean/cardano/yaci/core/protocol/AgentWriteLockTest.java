package com.bloxbean.cardano.yaci.core.protocol;

import com.bloxbean.cardano.yaci.core.protocol.keepalive.KeepAliveAgent;
import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultEventLoopGroup;
import io.netty.channel.DefaultMessageSizeEstimator;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MessageSizeEstimator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.local.LocalAddress;
import io.netty.channel.local.LocalChannel;
import io.netty.channel.local.LocalServerChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.lang.management.LockInfo;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class AgentWriteLockTest {

    private static final int SEGMENT = 65535;
    private static final byte FIRST = 1;
    private static final byte SECOND = 2;

    /**
     * A write that completes on the calling thread runs its success callback there. The callback takes the agent
     * lock, so it must not run while the channel lock is held: agent-locked writers take the locks in the opposite
     * order, and the two threads deadlock.
     */
    @Test
    void successCallbackRunsWithoutChannelLock() {
        EmbeddedChannel channel = new EmbeddedChannel();
        AtomicReference<Boolean> channelLockHeld = new AtomicReference<>();
        KeepAliveAgent agent = new KeepAliveAgent() {
            @Override
            public synchronized void sendRequest(Message message) {
                channelLockHeld.set(Thread.holdsLock(channel));
                super.sendRequest(message);
            }
        };
        agent.setChannel(channel);

        agent.sendNextMessage();

        assertThat(channel.<Object>readOutbound()).isInstanceOf(Segment.class);
        assertThat(channelLockHeld.get()).isFalse();
        channel.finishAndReleaseAll();
    }

    // A write the socket cannot take yet stays pending; a later write's flush completes it and runs its callback
    // on the thread of that later write.

    @Test
    void pendingWriteCompletedBySingleSegmentWrite() {
        assertPendingWriteCallbackRunsWithoutChannelLock(10, 10);
    }

    @Test
    void pendingWriteCompletedBySegmentedWrite() {
        assertPendingWriteCallbackRunsWithoutChannelLock(10, 2 * SEGMENT);
    }

    @Test
    void pendingSegmentedWriteCompletedBySingleSegmentWrite() {
        assertPendingWriteCallbackRunsWithoutChannelLock(2 * SEGMENT, 10);
    }

    private static void assertPendingWriteCallbackRunsWithoutChannelLock(int firstSize, int secondSize) {
        DelayedWriteChannel channel = new DelayedWriteChannel();
        TestAgent first = new TestAgent(channel);
        TestAgent second = new TestAgent(channel);
        List<Boolean> channelLockHeld = new ArrayList<>();

        first.write(payload(firstSize), () -> channelLockHeld.add(Thread.holdsLock(channel)));
        assertThat(channelLockHeld).as("first write still pending").isEmpty();

        channel.drain = true;
        second.write(payload(secondSize), null);

        assertThat(channelLockHeld).containsExactly(false);
        channel.finishAndReleaseAll();
    }

    @Test
    void largeMessageIsWrittenAsSegmentsInOrderAndCallsBackOnce() {
        EmbeddedChannel channel = new EmbeddedChannel();
        TestAgent agent = new TestAgent(channel);
        AtomicInteger callbacks = new AtomicInteger();
        byte[] payload = payload(2 * SEGMENT + 10);

        agent.write(payload, callbacks::incrementAndGet);

        ByteArrayOutputStream received = new ByteArrayOutputStream();
        List<Integer> sizes = new ArrayList<>();
        Segment segment;
        while ((segment = channel.readOutbound()) != null) {
            assertThat(segment.getProtocol()).isEqualTo(TestAgent.PROTOCOL_ID);
            sizes.add(segment.getPayload().length);
            received.writeBytes(segment.getPayload());
        }
        assertThat(sizes).containsExactly(SEGMENT, SEGMENT, 10);
        assertThat(received.toByteArray()).isEqualTo(payload);
        assertThat(callbacks).hasValue(1);
        channel.finishAndReleaseAll();
    }

    /**
     * The deadlock on a real event loop: agent A has a write pending, and a thread holding A's lock writes on the
     * same channel while the event loop's next write completes A's pending write, whose callback needs A's lock.
     * Neither side may wait for the other.
     */
    @Test
    void writerHoldingAgentLockDoesNotDeadlockWithCallbackOnEventLoop() throws Exception {
        EventLoopGroup group = eventLoopGroup();
        try {
            HoldingHandler holding = new HoldingHandler();
            Channel channel = connect(group, holding);
            TestAgent a = new TestAgent(channel);
            TestAgent b = new TestAgent(channel);
            CountDownLatch callbackRan = new CountDownLatch(1);

            a.write(payload(10), () -> {
                synchronized (a) {
                    callbackRan.countDown();
                }
            });
            assertThat(holding.held.await(5, TimeUnit.SECONDS)).as("A's write pending").isTrue();

            Thread writer = new Thread(() -> {
                synchronized (a) {
                    holding.release = true;
                    channel.eventLoop().execute(() -> b.write(payload(10), null));
                    try {
                        holding.flushing.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    a.write(payload(10), null);
                }
            });
            writer.setDaemon(true);
            writer.start();

            assertThat(callbackRan.await(5, TimeUnit.SECONDS)).as("A's callback ran").isTrue();
            writer.join(5000);
            assertThat(writer.isAlive()).isFalse();
            channel.close().sync();
        } finally {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }
    }

    /**
     * Writes queued from another thread count against the channel's writability before the event loop runs them, so
     * a producer that checks {@code isWritable()} (BlockFetchServerAgent) slows down when the event loop falls behind.
     * Netty counts each queued write as 40 bytes (8 for a message of unknown size plus 32 task overhead): 2,000 writes
     * pass the default 64 KiB high water mark.
     */
    @Test
    void writesQueuedOffTheEventLoopCountAgainstWritability() throws Exception {
        EventLoopGroup group = eventLoopGroup();
        CountDownLatch resume = new CountDownLatch(1);
        try {
            Channel channel = connect(group, new ChannelOutboundHandlerAdapter());
            TestAgent agent = new TestAgent(channel);
            CountDownLatch paused = new CountDownLatch(1);
            channel.eventLoop().execute(() -> {
                paused.countDown();
                try {
                    resume.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(paused.await(5, TimeUnit.SECONDS)).as("event loop paused").isTrue();

            for (int i = 0; i < 2000; i++)
                agent.write(payload(10), null);

            assertThat(channel.isWritable()).isFalse();
        } finally {
            resume.countDown();
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }
    }

    /**
     * Off the event loop each segment is queued as its own event-loop task, so two threads writing large messages on
     * one channel could interleave their segments, and the peer could reassemble neither message. The channel lock
     * keeps a message's segments together. Netty sizes a write queued off the event loop on the writing thread, so the
     * first writer is held there between its segments until the second writer has either queued a segment or is
     * blocked on the channel lock.
     */
    @Test
    void largeMessagesWrittenConcurrentlyOffTheEventLoopDoNotInterleave() throws Exception {
        EventLoopGroup group = eventLoopGroup();
        try {
            AtomicReference<Channel> channelRef = new AtomicReference<>();
            AtomicReference<Thread> secondWriter = new AtomicReference<>();
            AtomicInteger firstSegmentsSized = new AtomicInteger();
            AtomicInteger secondSegmentsSized = new AtomicInteger();
            CountDownLatch firstWriterHeld = new CountDownLatch(1);
            CountDownLatch secondWriterQueuedASegment = new CountDownLatch(1);
            AtomicReference<Boolean> secondWriterReachedTheChannel = new AtomicReference<>();
            MessageSizeEstimator.Handle sizes = msg -> {
                Channel ch = channelRef.get();
                if (msg instanceof Segment segment && ch != null && !ch.eventLoop().inEventLoop()) {
                    if (segment.getPayload()[0] == SECOND) {
                        // its first segment is queued once its second is being sized
                        if (secondSegmentsSized.incrementAndGet() == 2)
                            secondWriterQueuedASegment.countDown();
                    } else if (firstSegmentsSized.incrementAndGet() == 2) {
                        firstWriterHeld.countDown();
                        secondWriterReachedTheChannel.set(
                                awaitWriterAt(ch, secondWriter, secondWriterQueuedASegment));
                    }
                }
                return 8;
            };
            RecordingHandler recording = new RecordingHandler();
            Channel channel = connect(group, recording, () -> sizes);
            channelRef.set(channel);
            TestAgent first = new TestAgent(channel);
            TestAgent second = new TestAgent(channel);

            Thread firstWriter = new Thread(() -> first.write(payload(2 * SEGMENT + 10, FIRST), null));
            firstWriter.setDaemon(true);
            firstWriter.start();
            assertThat(firstWriterHeld.await(5, TimeUnit.SECONDS)).as("first writer between its segments").isTrue();
            Thread writer = new Thread(() -> second.write(payload(2 * SEGMENT + 10, SECOND), null));
            writer.setDaemon(true);
            secondWriter.set(writer);
            writer.start();
            firstWriter.join(5000);
            writer.join(5000);
            channel.eventLoop().submit(() -> {}).sync();

            assertThat(secondWriterReachedTheChannel.get()).as("second writer reached the channel").isTrue();
            assertThat(recording.markers).containsExactly(FIRST, FIRST, FIRST, SECOND, SECOND, SECOND);
            channel.close().sync();
        } finally {
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }
    }

    /** @return true once the writer has queued a segment or is blocked on the channel's lock, false after 5 s */
    private static boolean awaitWriterAt(Channel channel, AtomicReference<Thread> writer, CountDownLatch queued) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            Thread thread = writer.get();
            if (queued.getCount() == 0 || (thread != null && isBlockedOn(thread, channel)))
                return true;
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    private static boolean isBlockedOn(Thread thread, Object monitor) {
        ThreadInfo info = ManagementFactory.getThreadMXBean().getThreadInfo(thread.threadId());
        LockInfo lock = info != null ? info.getLockInfo() : null;
        return info != null && info.getThreadState() == Thread.State.BLOCKED && lock != null
                && lock.getIdentityHashCode() == System.identityHashCode(monitor);
    }

    private static EventLoopGroup eventLoopGroup() {
        // daemon threads: a deadlocked event loop must not keep the test JVM alive
        return new DefaultEventLoopGroup(1, new DefaultThreadFactory("agent-write-test", true));
    }

    private static Channel connect(EventLoopGroup group, ChannelHandler handler) throws InterruptedException {
        return connect(group, handler, DefaultMessageSizeEstimator.DEFAULT);
    }

    private static Channel connect(EventLoopGroup group, ChannelHandler handler, MessageSizeEstimator estimator)
            throws InterruptedException {
        LocalAddress address = new LocalAddress("agent-write-" + UUID.randomUUID());
        new ServerBootstrap().group(group).channel(LocalServerChannel.class)
                .childHandler(new ChannelInboundHandlerAdapter()).bind(address).sync();
        return new Bootstrap().group(group).channel(LocalChannel.class)
                .option(ChannelOption.MESSAGE_SIZE_ESTIMATOR, estimator).handler(handler).connect(address).sync()
                .channel();
    }

    private static byte[] payload(int size) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++)
            bytes[i] = (byte) i;
        return bytes;
    }

    private static byte[] payload(int size, byte marker) {
        byte[] bytes = new byte[size];
        Arrays.fill(bytes, marker);
        return bytes;
    }

    private static final class TestAgent extends Agent<AgentListener> {
        static final int PROTOCOL_ID = 8;

        TestAgent(Channel channel) {
            super(true);
            setChannel(channel);
        }

        void write(byte[] payload, Runnable onSuccess) {
            writeMessage(new Message<Void>() {
                @Override
                public byte[] serialize() {
                    return payload;
                }
            }, onSuccess);
        }

        @Override
        public int getProtocolId() {
            return PROTOCOL_ID;
        }

        @Override
        public boolean isDone() {
            return false;
        }

        @Override
        protected Message buildNextMessage() {
            return null;
        }

        @Override
        protected void processResponse(Message message) {
        }
    }

    /** Keeps flushed writes pending until {@code drain} is set, as a socket that cannot take them yet does. */
    private static final class DelayedWriteChannel extends EmbeddedChannel {
        boolean drain;

        @Override
        protected void doWrite(ChannelOutboundBuffer in) throws Exception {
            if (drain)
                super.doWrite(in);
        }
    }

    /**
     * Holds writes until {@code release} is set; the next flush then passes them on and completes them. {@code held}
     * counts the first flush that held writes.
     */
    private static final class HoldingHandler extends ChannelOutboundHandlerAdapter {
        final CountDownLatch held = new CountDownLatch(1);
        final CountDownLatch flushing = new CountDownLatch(1);
        final List<Object[]> pending = new ArrayList<>();
        volatile boolean release;

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            pending.add(new Object[]{msg, promise});
        }

        @Override
        public void flush(ChannelHandlerContext ctx) {
            if (!release) {
                held.countDown();
                return;
            }
            flushing.countDown();
            for (Object[] write : pending)
                ctx.write(write[0], (ChannelPromise) write[1]);
            pending.clear();
            ctx.flush();
        }
    }

    /** Records the first payload byte of each segment in the order the event loop writes them, and completes it. */
    private static final class RecordingHandler extends ChannelOutboundHandlerAdapter {
        final List<Byte> markers = new CopyOnWriteArrayList<>();

        @Override
        public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) {
            markers.add(((Segment) msg).getPayload()[0]);
            promise.setSuccess();
        }
    }
}
