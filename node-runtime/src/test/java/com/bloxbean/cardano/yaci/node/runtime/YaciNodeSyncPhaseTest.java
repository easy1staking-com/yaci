package com.bloxbean.cardano.yaci.node.runtime;

import com.bloxbean.cardano.yaci.core.model.Block;
import com.bloxbean.cardano.yaci.core.model.BlockHeader;
import com.bloxbean.cardano.yaci.core.model.Era;
import com.bloxbean.cardano.yaci.core.model.HeaderBody;
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Point;
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Tip;
import com.bloxbean.cardano.yaci.core.util.HexUtil;
import com.bloxbean.cardano.yaci.events.api.EventBus;
import com.bloxbean.cardano.yaci.events.api.SubscriptionOptions;
import com.bloxbean.cardano.yaci.helper.PeerClient;
import com.bloxbean.cardano.yaci.node.api.SyncPhase;
import com.bloxbean.cardano.yaci.node.api.config.YaciNodeConfig;
import com.bloxbean.cardano.yaci.node.api.events.SyncStatusChangedEvent;
import com.bloxbean.cardano.yaci.node.runtime.chain.InMemoryChainState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * When a pipelined {@link YaciNode} leaves INITIAL_SYNC for STEADY_STATE (YRB-4). The distance to the tip is measured
 * from the stored body tip to the live network tip ({@link SyncTipContext}, fed by ChainSync), falling back to the
 * startup {@code remoteTip} only while no header has arrived; {@code lastProcessedSlot} is never assigned and must not
 * be used. The live bug: a node ~32k slots behind logged "distance to tip: 202773005 slots" and, once in bulk mode,
 * never left it.
 */
class YaciNodeSyncPhaseTest {

    private static final long BASE_SLOT = 170_000_000L;
    private static final long BASE_BLOCK = 100L;

    private final List<String> phaseEvents = new CopyOnWriteArrayList<>();
    private InMemoryChainState chainState;
    private BodyFetchManager bfm;
    private PipelineDataListener listener;
    private YaciNode node;

    @BeforeEach
    void setUp() throws Exception {
        node = new YaciNode(YaciNodeConfig.builder()
                .remoteHost("localhost")
                .remotePort(3001)
                .protocolMagic(764824073)
                .serverPort(0)
                .useRocksDB(false)
                .build());
        EventBus bus = (EventBus) get("eventBus");
        bus.subscribe(SyncStatusChangedEvent.class,
                ctx -> phaseEvents.add(ctx.event().previous() + "->" + ctx.event().current()),
                SubscriptionOptions.builder().build());

        chainState = new InMemoryChainState();
        chainState.storeBlockHeader(hash(BASE_BLOCK), BASE_BLOCK, BASE_SLOT, new byte[]{1});
        chainState.storeBlock(hash(BASE_BLOCK), BASE_BLOCK, BASE_SLOT, new byte[]{1});
        bfm = new BodyFetchManager(new PeerClient("localhost", 1, 764824073L, Point.ORIGIN), chainState, bus,
                100, 5000, 500, 1000);
        listener = new PipelineDataListener(null, bfm, node);

        set("chainState", chainState);
        set("bodyFetchManager", bfm);
        set("isPipelinedMode", true);
        set("syncPhase", SyncPhase.INITIAL_SYNC);
    }

    @AfterEach
    void tearDown() throws Exception {
        ((ScheduledExecutorService) get("scheduler")).shutdownNow();
    }

    /** (e) A stored body leaves the node 500 slots behind the live network tip: STEADY_STATE, once. */
    @Test
    void initialSync_bodyStored500SlotsBehindNetworkTip_transitionsToSteadyStateOnce() throws Exception {
        set("remoteTip", tip(BASE_SLOT + 1_000_000L));     // startup tip, stale and far ahead
        networkTip(BASE_SLOT + 20 + 500);

        deliverBody(BASE_BLOCK + 1, BASE_SLOT + 20);

        assertThat(get("syncPhase")).isEqualTo(SyncPhase.STEADY_STATE);
        assertThat(bfm.getSyncPhase()).isEqualTo(SyncPhase.STEADY_STATE);
        assertThat(phaseEvents).containsExactly("INITIAL_SYNC->STEADY_STATE");

        deliverBody(BASE_BLOCK + 2, BASE_SLOT + 40);
        assertThat(chainState.getTip().getBlockNumber()).isEqualTo(BASE_BLOCK + 2);
        assertThat(phaseEvents).as("the transition is published once").containsExactly("INITIAL_SYNC->STEADY_STATE");
    }

    /** (e) No header has arrived yet: the startup remote tip is the network tip. */
    @Test
    void initialSync_noLiveNetworkTip_measuresFromTheStartupRemoteTip() throws Exception {
        set("remoteTip", tip(BASE_SLOT + 20 + 500));

        deliverBody(BASE_BLOCK + 1, BASE_SLOT + 20);

        assertThat(get("syncPhase")).isEqualTo(SyncPhase.STEADY_STATE);
        assertThat(bfm.getSyncPhase()).isEqualTo(SyncPhase.STEADY_STATE);
        assertThat(phaseEvents).containsExactly("INITIAL_SYNC->STEADY_STATE");
    }

    /** (f) 5000 slots behind the live network tip: stays in INITIAL_SYNC, even with a stale remote tip behind us. */
    @Test
    void initialSync_bodyStored5000SlotsBehindNetworkTip_staysInInitialSync() throws Exception {
        set("remoteTip", tip(BASE_SLOT));                  // startup tip, stale and behind the stored tip
        networkTip(BASE_SLOT + 20 + 5000);

        deliverBody(BASE_BLOCK + 1, BASE_SLOT + 20);

        assertThat(chainState.getTip().getBlockNumber()).isEqualTo(BASE_BLOCK + 1);
        assertThat(get("syncPhase")).isEqualTo(SyncPhase.INITIAL_SYNC);
        assertThat(bfm.getSyncPhase()).isEqualTo(SyncPhase.INITIAL_SYNC);
        assertThat(phaseEvents).isEmpty();
    }

    /** (g) FAST RESUME after an intersection measures from the stored tip and the live network tip. */
    @Test
    void fastResume_measuresFromStoredTipAndNetworkTip_notLastProcessedSlot() throws Exception {
        set("remoteTip", tip(BASE_SLOT + 1_000_000L));     // startup tip, stale and far ahead
        assertThat(get("lastProcessedSlot")).isEqualTo(0L);
        networkTip(BASE_SLOT + 500);
        set("syncPhase", SyncPhase.INTERSECT_PHASE);
        bfm.pause();

        node.resumeBodyFetchOnHeaderFlow();

        assertThat(get("syncPhase")).isEqualTo(SyncPhase.STEADY_STATE);
        assertThat(bfm.getSyncPhase()).isEqualTo(SyncPhase.STEADY_STATE);
        assertThat(bfm.isPaused()).isFalse();
        assertThat(phaseEvents).containsExactly("INTERSECT_PHASE->STEADY_STATE");
    }

    /** (g) FAST RESUME before any header has arrived: the startup remote tip, not lastProcessedSlot (0). */
    @Test
    void fastResume_noLiveNetworkTip_measuresFromTheStartupRemoteTip() throws Exception {
        set("remoteTip", tip(BASE_SLOT + 500));
        set("syncPhase", SyncPhase.INTERSECT_PHASE);
        bfm.pause();

        node.resumeBodyFetchOnHeaderFlow();

        assertThat(get("syncPhase")).isEqualTo(SyncPhase.STEADY_STATE);
        assertThat(bfm.getSyncPhase()).isEqualTo(SyncPhase.STEADY_STATE);
    }

    /** (g) FAST RESUME 5000 slots behind the network tip resumes into INITIAL_SYNC. */
    @Test
    void fastResume_farFromNetworkTip_resumesIntoInitialSync() throws Exception {
        set("remoteTip", tip(BASE_SLOT));
        networkTip(BASE_SLOT + 5000);
        set("syncPhase", SyncPhase.INTERSECT_PHASE);
        bfm.pause();

        node.resumeBodyFetchOnHeaderFlow();

        assertThat(get("syncPhase")).isEqualTo(SyncPhase.INITIAL_SYNC);
        assertThat(bfm.getSyncPhase()).isEqualTo(SyncPhase.INITIAL_SYNC);
        assertThat(phaseEvents).containsExactly("INTERSECT_PHASE->INITIAL_SYNC");
    }

    /** (g) The intersection-phase timeout measures from the stored tip and the live network tip. */
    @Test
    void intersectionTimeout_measuresFromStoredTipAndNetworkTip_notLastProcessedSlot() throws Exception {
        set("remoteTip", tip(BASE_SLOT + 1_000_000L));
        networkTip(BASE_SLOT + 500);
        set("rollbackClassificationTimeout", 50L);

        node.onIntersectionFound();
        // The task writes YaciNode.syncPhase, then the volatile BodyFetchManager phase: wait on the latter
        long deadline = System.currentTimeMillis() + 5000;
        while (bfm.getSyncPhase() == SyncPhase.INTERSECT_PHASE && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        assertThat(get("syncPhase")).isEqualTo(SyncPhase.STEADY_STATE);
        assertThat(bfm.getSyncPhase()).isEqualTo(SyncPhase.STEADY_STATE);
    }

    /** Delivers header then body through the pipelined listener, as BlockFetch does after ChainSync stored the header. */
    private void deliverBody(long blockNumber, long slot) {
        chainState.storeBlockHeader(hash(blockNumber), blockNumber, slot, new byte[]{(byte) blockNumber});
        Block block = Block.builder().era(Era.Conway).cbor("82" + String.format("%02x", blockNumber & 0xff))
                .header(BlockHeader.builder().headerBody(HeaderBody.builder()
                        .blockNumber(blockNumber).slot(slot).blockHash(HexUtil.encodeHexString(hash(blockNumber)))
                        .build()).build())
                .build();
        listener.onBlock(Era.Conway, block, List.of());
    }

    private void networkTip(long slot) throws Exception {
        ((SyncTipContext) get("syncTipContext")).update(tip(slot));
    }

    private static Tip tip(long slot) {
        return new Tip(new Point(slot, HexUtil.encodeHexString(hash(slot))), slot);
    }

    private static byte[] hash(long n) {
        return HexUtil.decodeHexString(String.format("%064x", n));
    }

    private void set(String name, Object value) throws Exception {
        field(name).set(node, value);
    }

    private Object get(String name) throws Exception {
        return field(name).get(node);
    }

    private static Field field(String name) throws Exception {
        Field field = YaciNode.class.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }
}
