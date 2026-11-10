package com.bloxbean.cardano.yaci.node.runtime;

import com.bloxbean.cardano.yaci.core.network.server.NodeServer;
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Point;
import com.bloxbean.cardano.yaci.core.storage.ChainState;
import com.bloxbean.cardano.yaci.core.storage.ChainTip;
import com.bloxbean.cardano.yaci.core.util.HexUtil;
import com.bloxbean.cardano.yaci.events.api.EventBus;
import com.bloxbean.cardano.yaci.events.api.SubscriptionOptions;
import com.bloxbean.cardano.yaci.node.api.SyncPhase;
import com.bloxbean.cardano.yaci.node.api.config.YaciNodeConfig;
import com.bloxbean.cardano.yaci.node.api.events.RollbackEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * How {@link YaciNode#handleRollback(Point)} classifies a rollback from the upstream peer, and when it tells the N2N
 * server. The chain is the 2026-11-09 mainnet slot battle: the node had block 14177494 at slot 202645599, and the
 * peer rolled it back to its parent at slot 202645590 (nine slots, one block) before sending the canonical block.
 * <p>
 * A real rollback must reach the server after the chain state is rolled back, so the chain-sync server finds the
 * client's last block gone and sends the Rollbackward at once instead of when the replacement block is stored.
 */
class YaciNodeRollbackClassificationTest {

    private static final Point PARENT = new Point(202645590,
            "120a52b3ebc1cacb20cc857621067c8c98b582d26d0f48e4f413649fc558a7ea");
    private static final Point ORPHAN = new Point(202645599,
            "3a345786691cd41264aa622fd78538d42260541f9caa26a20776e7217dbc65d5");
    private static final Point CANONICAL = new Point(202645599,
            "c791bad256962f34b7bc5e11c95e46a5682604381605388d0912162cb55f82a0");

    private final List<String> events = new CopyOnWriteArrayList<>();
    private RecordingChainState chainState;
    private RecordingNodeServer nodeServer;
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

        chainState = new RecordingChainState();
        chainState.add(PARENT, 14177493);
        chainState.add(ORPHAN, 14177494);
        nodeServer = new RecordingNodeServer();

        set("chainState", chainState);
        set("nodeServer", nodeServer);
        ((AtomicBoolean) get("isServerRunning")).set(true);
        set("syncPhase", SyncPhase.STEADY_STATE);
        set("lastKnownChainTip", chainState.getTip());
        ((EventBus) get("eventBus")).subscribe(RollbackEvent.class,
                ctx -> events.add("event realReorg=" + ctx.event().realReorg()), SubscriptionOptions.builder().build());
    }

    @AfterEach
    void tearDown() {
        nodeServer.shutdown();
    }

    @Test
    void steadyStateOneBlockRollback_isRealAndNotifiesTheServerAfterRollingBack() {
        node.handleRollback(PARENT);

        assertThat(events).containsExactly(
                "rollbackTo " + PARENT.getSlot(),
                "event realReorg=true",
                "notify tip=" + PARENT.getSlot());
    }

    /** The peer names a different block in the slot of our tip: our tip is not on its chain. */
    @Test
    void sameSlotDifferentHash_isRealAndNotifiesTheServerAfterRollingBack() {
        node.handleRollback(CANONICAL);

        assertThat(events).containsExactly(
                "rollbackTo " + CANONICAL.getSlot(),
                "event realReorg=true",
                "notify tip=" + CANONICAL.getSlot());
    }

    /** A rollback to our own tip (the reply to a reconnecting FindIntersect) discards nothing. */
    @Test
    void rollbackToOurTip_isNotReal() {
        node.handleRollback(ORPHAN);

        assertThat(events).containsExactly("rollbackTo " + ORPHAN.getSlot(), "event realReorg=false");
    }

    @Test
    void rollbackDuringInitialSync_isNotReal() throws Exception {
        set("syncPhase", SyncPhase.INITIAL_SYNC);

        node.handleRollback(PARENT);

        assertThat(events).containsExactly("rollbackTo " + PARENT.getSlot(), "event realReorg=false");
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

    /** Records each notification with the tip the server agents would read. */
    private final class RecordingNodeServer extends NodeServer {
        RecordingNodeServer() {
            super(0, null, null);
        }

        @Override
        public void notifyNewDataAvailable() {
            events.add("notify tip=" + chainState.getTip().getSlot());
        }
    }

    /** Blocks by slot; records each rollback. */
    private final class RecordingChainState implements ChainState {
        private final TreeMap<Long, ChainTip> blocks = new TreeMap<>();

        void add(Point point, long blockNumber) {
            blocks.put(point.getSlot(), new ChainTip(point.getSlot(), HexUtil.decodeHexString(point.getHash()),
                    blockNumber));
        }

        @Override
        public void rollbackTo(Long slot) {
            events.add("rollbackTo " + slot);
            blocks.tailMap(slot, false).clear();
        }

        @Override
        public ChainTip getTip() {
            Map.Entry<Long, ChainTip> last = blocks.lastEntry();
            return last != null ? last.getValue() : null;
        }

        @Override
        public ChainTip getHeaderTip() {
            return getTip();
        }

        @Override
        public boolean hasPoint(Point point) {
            ChainTip block = blocks.get(point.getSlot());
            return block != null && HexUtil.encodeHexString(block.getBlockHash()).equals(point.getHash());
        }

        @Override public void storeBlock(byte[] blockHash, Long blockNumber, Long slot, byte[] block) {}
        @Override public byte[] getBlock(byte[] blockHash) { return null; }
        @Override public boolean hasBlock(byte[] blockHash) { return false; }
        @Override public void storeBlockHeader(byte[] blockHash, Long blockNumber, Long slot, byte[] blockHeader) {}
        @Override public byte[] getBlockHeader(byte[] blockHash) { return null; }
        @Override public byte[] getBlockByNumber(Long blockNumber) { return null; }
        @Override public byte[] getBlockHeaderByNumber(Long blockNumber) { return null; }
        @Override public Point findNextBlock(Point currentPoint) { return null; }
        @Override public Point findNextBlockHeader(Point currentPoint) { return null; }
        @Override public List<Point> findBlocksInRange(Point from, Point to) { return List.of(); }
        @Override public Point findLastPointAfterNBlocks(Point from, long batchSize) { return null; }
        @Override public Point getFirstBlock() { return null; }
        @Override public Long getBlockNumberBySlot(Long slot) { return null; }
        @Override public Long getSlotByBlockNumber(Long blockNumber) { return null; }
    }
}
