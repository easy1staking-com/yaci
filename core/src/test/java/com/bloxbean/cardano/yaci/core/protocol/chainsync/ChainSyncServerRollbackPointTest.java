package com.bloxbean.cardano.yaci.core.protocol.chainsync;

import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.*;
import com.bloxbean.cardano.yaci.core.protocol.chainsync.n2n.ChainSyncAgentListener;
import com.bloxbean.cardano.yaci.core.protocol.chainsync.n2n.ChainSyncServerAgent;
import com.bloxbean.cardano.yaci.core.protocol.chainsync.n2n.ChainSyncState;
import com.bloxbean.cardano.yaci.core.storage.ChainState;
import com.bloxbean.cardano.yaci.core.storage.ChainTip;
import com.bloxbean.cardano.yaci.core.util.HexUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The point a follower is rolled back to after our chain switches to a short fork, on real mainnet slot spacing
 * (blocks are about 20 slots apart, so most slots hold no block).
 * <p>
 * The follower is the downstream peer of our N2N ChainSync server. It has been sent blocks up to our tip and waits
 * there (MustReply). Our chain then rolls back and adopts a fork. The follower must be rolled back to the newest
 * block it has that is still on our chain: never further, and never to {@code Point.ORIGIN}, which a Haskell node
 * answers with MsgDone and a demotion of the peer.
 * <p>
 * The fixture {@code yrb-mainnet-blocks.csv} holds 788 consecutive mainnet blocks (slot, height, hash), slots
 * 202630061..202645897, from api.koios.rest {@code /blocks}. The 2026-11-09 08:11:30 incident: our tip was block
 * 14177494 at slot 202645599 with hash 3a345786..., an orphan of a slot battle; the canonical block at that slot is
 * c791bad2... and its parent, the fork point, is slot 202645590, hash 120a52b3....
 */
class ChainSyncServerRollbackPointTest {

    static final long INCIDENT_SLOT = 202645599L;
    static final String INCIDENT_ORPHAN = "3a345786691cd41264aa622fd78538d42260541f9caa26a20776e7217dbc65d5";
    static final Point INCIDENT_FORK_POINT =
            new Point(202645590L, "120a52b3ebc1cacb20cc857621067c8c98b582d26d0f48e4f413649fc558a7ea");

    record B(long slot, long height, String hash) {
        Point point() {
            return new Point(slot, hash);
        }

        B withHash(String other) {
            return new B(slot, height, other);
        }
    }

    static List<B> mainnet;
    static byte[] fixtureBlock;

    @BeforeAll
    static void load() throws Exception {
        List<B> out = new ArrayList<>();
        try (var in = ChainSyncServerRollbackPointTest.class.getResourceAsStream("/yrb-mainnet-blocks.csv");
             var r = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) {
                if (line.startsWith("#") || line.isBlank()) continue;
                String[] p = line.split(",");
                out.add(new B(Long.parseLong(p[0]), Long.parseLong(p[1]), p[2]));
            }
        }
        mainnet = List.copyOf(out);
        // Only read if a slot search finds a block: the search makes its rollback point from the stored block
        try (var in = ChainSyncServerRollbackPointTest.class.getResourceAsStream("/block/preprod286853.txt")) {
            fixtureBlock = HexUtil.decodeHexString(new String(in.readAllBytes(), StandardCharsets.UTF_8).trim());
        }
    }

    /**
     * A hash that differs from {@code hash} in its first hex digit: a sibling block at the same slot. (Replacing the
     * first two digits with "ff" left the hash unchanged for the two fixture blocks whose hash starts with ff.)
     */
    static String siblingHash(String hash) {
        int digit = Character.digit(hash.charAt(0), 16);
        return Character.forDigit((digit + 1) % 16, 16) + hash.substring(1);
    }

    static int indexOfSlot(long slot) {
        for (int i = 0; i < mainnet.size(); i++)
            if (mainnet.get(i).slot() == slot) return i;
        throw new IllegalArgumentException("slot not in fixture: " + slot);
    }

    // (a) The incident: a one-block slot battle at the tip, found when the replacement block is stored

    @Test
    void incident_followerAtTip_isRolledBackToForkPoint_notOrigin() {
        int tip = indexOfSlot(INCIDENT_SLOT);
        assertThat(mainnet.get(tip - 1).point()).isEqualTo(INCIDENT_FORK_POINT);

        // Production indexes slot 0 (no "could not find any valid rollback point" error was logged); without it the
        // slot search answered AwaitReply instead of ORIGIN. The answer must not depend on it.
        for (boolean slot0Indexed : new boolean[]{true, false}) {
            Server server = new Server(slot0Indexed);
            server.chain(mainnet.subList(0, tip));
            server.add(mainnet.get(tip).withHash(INCIDENT_ORPHAN));
            server.followToTip(mainnet.get(tip - 1));
            assertThat(server.agent.getLastSentPoint()).isEqualTo(new Point(INCIDENT_SLOT, INCIDENT_ORPHAN));

            server.switchFork(INCIDENT_FORK_POINT.getSlot(), List.of(mainnet.get(tip)));
            server.agent.onNewDataAvailable();

            assertThat(server.rollbacks).as("slot0Indexed=%s", slot0Indexed).containsExactly(INCIDENT_FORK_POINT);
            assertThat(server.agent.getLastSentPoint()).isEqualTo(INCIDENT_FORK_POINT);
            assertThat(server.agent.getCurrentState()).isNotEqualTo(ChainSyncState.Done);
        }
    }

    @Test
    void incident_rollbackFoundOnTheNextRequest_isRolledBackToForkPoint() {
        int tip = indexOfSlot(INCIDENT_SLOT);
        Server server = new Server(true);
        server.chain(mainnet.subList(0, tip));
        server.add(mainnet.get(tip).withHash(INCIDENT_ORPHAN));
        server.intersect(mainnet.get(tip - 1));
        server.requestNext();
        assertThat(server.agent.getLastSentPoint()).isEqualTo(new Point(INCIDENT_SLOT, INCIDENT_ORPHAN));

        // The fork switch happens before the follower asks for the next block
        server.switchFork(INCIDENT_FORK_POINT.getSlot(), List.of(mainnet.get(tip)));
        server.rollbacks.clear();
        server.requestNext();

        assertThat(server.rollbacks).containsExactly(INCIDENT_FORK_POINT);
    }

    // (b) Every block of the fixture as a one-block rollback at the tip

    @Test
    void sweep_everyMainnetBlockAsOneBlockRollback_rollsBackToItsParent() {
        long firstSlot = mainnet.get(0).slot();
        int scenarios = 0;
        int origin = 0, parentSlot = 0, olderBlock = 0, noRollback = 0;
        List<String> wrong = new ArrayList<>();
        for (int i = 1; i < mainnet.size(); i++) {
            B tip = mainnet.get(i);
            // The slot search reached up to 5000 slots back: keep only tips with that much history before them, so
            // the cases are the same before and after the fix
            if (tip.slot() - 5000 <= firstSlot) continue;
            scenarios++;
            B parent = mainnet.get(i - 1);

            Server server = new Server(true);
            server.chain(mainnet.subList(0, i));
            server.add(tip.withHash(siblingHash(tip.hash())));
            server.followToTip(parent);
            server.switchFork(parent.slot(), List.of(tip));
            server.agent.onNewDataAvailable();

            List<Point> got = server.rollbacks;
            if (got.equals(List.of(parent.point()))) continue;
            if (got.isEmpty()) noRollback++;
            else if (got.get(0).getSlot() == 0 && got.get(0).getHash() == null) origin++;
            else if (got.get(0).getSlot() == parent.slot()) parentSlot++; // with the hash of the stored test block
            else olderBlock++;
            if (wrong.size() < 5) wrong.add("tip slot " + tip.slot() + ": expected " + parent.point() + ", got " + got);
        }

        assertThat(scenarios).isEqualTo(524);
        assertThat(wrong)
                .as("of %d one-block rollbacks: %d to ORIGIN, %d to an older block, %d to the parent's slot with "
                                + "another hash, %d without a rollback", scenarios, origin, olderBlock, parentSlot, noRollback)
                .isEmpty();
    }

    // (c) The fork's block shares the orphan's slot: the follower must then be sent it, not skip the slot

    @Test
    void sameSlotSibling_isSentAfterTheRollback() {
        int tip = indexOfSlot(INCIDENT_SLOT);
        B canonical = mainnet.get(tip);
        B next = mainnet.get(tip + 1);
        Server server = new Server(true);
        server.chain(mainnet.subList(0, tip));
        server.add(canonical.withHash(INCIDENT_ORPHAN));
        server.followToTip(mainnet.get(tip - 10));

        server.switchFork(INCIDENT_FORK_POINT.getSlot(), List.of(canonical, next));
        server.agent.onNewDataAvailable();
        assertThat(server.rollbacks).containsExactly(INCIDENT_FORK_POINT);

        server.requestNext();
        assertThat(server.agent.getLastSentPoint()).isEqualTo(canonical.point());
        server.requestNext();
        assertThat(server.agent.getLastSentPoint()).isEqualTo(next.point());
    }

    @Test
    void siblingAtALaterSlot_rollsBackToForkPoint() {
        int tip = indexOfSlot(INCIDENT_SLOT);
        B orphan = mainnet.get(tip).withHash(INCIDENT_ORPHAN);
        // The fork's block is the canonical block after the incident one, as if the incident slot had been empty
        B laterSibling = new B(mainnet.get(tip + 1).slot(), orphan.height(), mainnet.get(tip + 1).hash());
        Server server = new Server(true);
        server.chain(mainnet.subList(0, tip));
        server.add(orphan);
        server.followToTip(mainnet.get(tip - 1));

        server.switchFork(INCIDENT_FORK_POINT.getSlot(), List.of(laterSibling));
        server.agent.onNewDataAvailable();

        assertThat(server.rollbacks).containsExactly(INCIDENT_FORK_POINT);
        server.requestNext();
        assertThat(server.agent.getLastSentPoint()).isEqualTo(laterSibling.point());
    }

    // (d) Deeper rollbacks: every block the follower has after the fork point is orphaned

    @Test
    void rollbackOfTwoBlocks_rollsBackToForkPoint() {
        assertRollbackOfNBlocks(2);
    }

    @Test
    void rollbackOfTenBlocks_rollsBackToForkPoint() {
        assertRollbackOfNBlocks(10);
    }

    private void assertRollbackOfNBlocks(int n) {
        int tip = indexOfSlot(INCIDENT_SLOT);
        int fork = tip - n;
        Server server = new Server(true);
        server.chain(mainnet.subList(0, fork + 1));
        List<B> orphans = new ArrayList<>();
        List<B> canonical = new ArrayList<>();
        for (int i = fork + 1; i <= tip; i++) {
            orphans.add(mainnet.get(i).withHash(siblingHash(mainnet.get(i).hash())));
            canonical.add(mainnet.get(i));
        }
        orphans.forEach(server::add);
        server.followToTip(mainnet.get(fork - 5));
        assertThat(server.agent.getLastSentPoint()).isEqualTo(orphans.get(n - 1).point());

        server.switchFork(mainnet.get(fork).slot(), canonical);
        server.agent.onNewDataAvailable();

        assertThat(server.rollbacks).as("rollback of %d blocks", n).containsExactly(mainnet.get(fork).point());
        server.requestNext();
        assertThat(server.agent.getLastSentPoint()).isEqualTo(canonical.get(0).point());
    }

    /** Our server: a chain state, the agent serving one follower, and the rollbacks the follower was sent. */
    static class Server {
        final SparseChainState chainState;
        final ChainSyncServerAgent agent;
        final List<Point> rollbacks = new ArrayList<>();

        Server(boolean slot0Indexed) {
            chainState = new SparseChainState(fixtureBlock, slot0Indexed);
            agent = new ChainSyncServerAgent(chainState);
            agent.setChannel(new ChainSyncServerAgentConcurrencyTest.StubChannel());
            agent.addListener(new ChainSyncAgentListener() {
                @Override
                public void rollbackward(Tip tip, Point toPoint) {
                    rollbacks.add(toPoint);
                }
            });
        }

        void chain(List<B> blocks) {
            blocks.forEach(this::add);
        }

        void add(B block) {
            chainState.add(block);
        }

        /** FindIntersect at {@code at}, then the first RequestNext, answered with a rollback to it. */
        void intersect(B at) {
            agent.receiveResponse(new FindIntersect(new Point[]{at.point()}));
            agent.sendNextMessage();
            requestNext();
            if (!rollbacks.equals(List.of(at.point())))
                throw new IllegalStateException("setup: rollbacks after FindIntersect " + rollbacks);
        }

        /** Intersect at {@code at}, then request blocks until the server has none left and the follower waits. */
        void followToTip(B at) {
            intersect(at);
            for (int i = 0; i < 100 && !agent.isClientAtTip(); i++)
                requestNext();
            if (agent.getCurrentState() != ChainSyncState.MustReply || !agent.isClientAtTip())
                throw new IllegalStateException("setup: follower not waiting at the tip, state " + agent.getCurrentState());
            rollbacks.clear();
        }

        void requestNext() {
            agent.receiveResponse(new RequestNext());
            agent.sendNextMessage();
        }

        /** Our chain rolls back to {@code forkSlot} and adopts {@code fork}. */
        void switchFork(long forkSlot, List<B> fork) {
            chainState.rollbackTo(forkSlot);
            fork.forEach(this::add);
        }
    }

    /**
     * Blocks on sparse real slots. As in DirectRocksDBChainState, {@code hasPoint} checks the slot's hash and
     * {@code getBlockNumberBySlot} finds only a slot that holds a block.
     */
    static class SparseChainState implements ChainState {
        final ConcurrentSkipListMap<Long, B> bySlot = new ConcurrentSkipListMap<>();
        final Map<Long, B> byHeight = new HashMap<>();
        final byte[] fixture;
        final boolean slot0Indexed;

        SparseChainState(byte[] fixture, boolean slot0Indexed) {
            this.fixture = fixture;
            this.slot0Indexed = slot0Indexed;
        }

        void add(B b) {
            bySlot.put(b.slot(), b);
            byHeight.put(b.height(), b);
        }

        static byte[] header(long height) {
            return new byte[]{(byte) 0x82, 0x07, 0x43, (byte) (height & 0xFF), (byte) ((height >> 8) & 0xFF), 0x01};
        }

        B byHash(String hex) {
            for (B b : bySlot.values())
                if (b.hash().equals(hex)) return b;
            return null;
        }

        @Override public void rollbackTo(Long slot) {
            bySlot.tailMap(slot, false).values().forEach(b -> byHeight.remove(b.height()));
            bySlot.tailMap(slot, false).clear();
        }
        @Override public boolean hasPoint(Point p) {
            if (p == null) return false;
            if (p.getSlot() == 0 && p.getHash() == null) return !bySlot.isEmpty();
            B b = bySlot.get(p.getSlot());
            return b != null && b.hash().equals(p.getHash());
        }
        @Override public Long getBlockNumberBySlot(Long slot) {
            if (slot == 0L) return slot0Indexed ? 0L : null;
            B b = bySlot.get(slot);
            return b == null ? null : b.height();
        }
        @Override public byte[] getBlockHeaderByNumber(Long n) { return byHeight.containsKey(n) ? header(n) : null; }
        @Override public byte[] getBlockByNumber(Long n) { return byHeight.containsKey(n) ? fixture : null; }
        @Override public byte[] getBlockHeader(byte[] hash) {
            B b = byHash(HexUtil.encodeHexString(hash));
            return b == null ? null : header(b.height());
        }
        @Override public byte[] getBlock(byte[] hash) { return byHash(HexUtil.encodeHexString(hash)) == null ? null : fixture; }
        @Override public boolean hasBlock(byte[] hash) { return byHash(HexUtil.encodeHexString(hash)) != null; }
        @Override public Point findNextBlock(Point p) {
            Map.Entry<Long, B> e = (p.getSlot() == 0 && p.getHash() == null)
                    ? bySlot.firstEntry() : bySlot.higherEntry(p.getSlot());
            return e == null ? null : e.getValue().point();
        }
        @Override public Point findNextBlockHeader(Point p) { return findNextBlock(p); }
        @Override public ChainTip getTip() {
            if (bySlot.isEmpty()) return null;
            B b = bySlot.lastEntry().getValue();
            return new ChainTip(b.slot(), HexUtil.decodeHexString(b.hash()), b.height());
        }
        @Override public ChainTip getHeaderTip() { return getTip(); }
        @Override public Point getFirstBlock() { return bySlot.firstEntry().getValue().point(); }
        @Override public Long getSlotByBlockNumber(Long n) { B b = byHeight.get(n); return b == null ? null : b.slot(); }
        @Override public void storeBlock(byte[] h, Long n, Long s, byte[] blk) {}
        @Override public void storeBlockHeader(byte[] h, Long n, Long s, byte[] hdr) {}
        @Override public List<Point> findBlocksInRange(Point from, Point to) { return List.of(); }
        @Override public Point findLastPointAfterNBlocks(Point from, long n) { return null; }
    }
}
