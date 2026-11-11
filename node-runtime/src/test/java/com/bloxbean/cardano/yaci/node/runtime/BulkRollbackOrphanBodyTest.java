package com.bloxbean.cardano.yaci.node.runtime;

import com.bloxbean.cardano.yaci.core.model.Block;
import com.bloxbean.cardano.yaci.core.model.BlockHeader;
import com.bloxbean.cardano.yaci.core.model.Era;
import com.bloxbean.cardano.yaci.core.model.HeaderBody;
import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Point;
import com.bloxbean.cardano.yaci.core.storage.ChainTip;
import com.bloxbean.cardano.yaci.core.util.HexUtil;
import com.bloxbean.cardano.yaci.events.api.EventBus;
import com.bloxbean.cardano.yaci.events.api.SubscriptionOptions;
import com.bloxbean.cardano.yaci.helper.PeerClient;
import com.bloxbean.cardano.yaci.node.api.SyncPhase;
import com.bloxbean.cardano.yaci.node.api.config.YaciNodeConfig;
import com.bloxbean.cardano.yaci.node.api.events.BlockAppliedEvent;
import com.bloxbean.cardano.yaci.node.api.events.BlockReceivedEvent;
import com.bloxbean.cardano.yaci.node.runtime.chain.DirectRocksDBChainState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * YRB-4: a one-block fork while a bulk (INITIAL_SYNC) body range is in flight. Headers run ahead of bodies; the peer
 * rolls the orphan header back ({@link YaciNode#handleRollback(Point)}, which only pauses the BodyFetchManager) and
 * sends a canonical replacement; the orphan's BODY, requested before the rollback, is then delivered through
 * {@link BodyFetchManager#onBlock}. The rollback deleted the orphan's header and slot index, so the store's FORK
 * MISMATCH check cannot see it: the BodyFetchManager must refuse a body whose header is not stored.
 * Synthetic chain #1..#n on a real RocksDB (hashes and CBOR are placeholders; only numbers/slots matter).
 */
class BulkRollbackOrphanBodyTest {

    @TempDir Path dir;
    private DirectRocksDBChainState chain;
    private BodyFetchManager bfm;
    private YaciNode node;
    private final List<String> received = new CopyOnWriteArrayList<>();
    private final List<String> applied = new CopyOnWriteArrayList<>();

    @BeforeEach
    void setUp() throws Exception {
        chain = new DirectRocksDBChainState(dir.resolve("db").toString());
        node = new YaciNode(YaciNodeConfig.builder().remoteHost("localhost").remotePort(3001)
                .protocolMagic(764824073).serverPort(0).useRocksDB(false).build());
        EventBus bus = (EventBus) get("eventBus");
        bus.subscribe(BlockReceivedEvent.class, ctx -> received.add(ctx.event().blockHash()),
                SubscriptionOptions.builder().build());
        bus.subscribe(BlockAppliedEvent.class, ctx -> applied.add(ctx.event().blockHash()),
                SubscriptionOptions.builder().build());
        bfm = new BodyFetchManager(new PeerClient("localhost", 1, 764824073L, Point.ORIGIN), chain, bus,
                100, 5000, 500, 1000);
        set("chainState", chain);
        set("bodyFetchManager", bfm);
        set("isPipelinedMode", true);
        set("syncPhase", SyncPhase.INITIAL_SYNC);

        for (int n = 1; n <= 4; n++) {
            header(n, slot(n), hash("a", n));
            body(n, slot(n), hash("a", n));
        }
        assertThat(chain.getTip().getBlockNumber()).isEqualTo(4);
    }

    @AfterEach
    void tearDown() {
        chain.close();
    }

    /** (a) Orphan #5 at slot 50; canonical #5 at slot 52 (a different slot, as most mainnet forks are). */
    @Test
    void orphanBodyDeliveredAfterRollbackAndCanonicalHeader_isRefused_andTheCanonicalChainIsStored() throws Exception {
        header(5, 50, hash("o", 5));                       // orphan header, body still in a bulk range in flight
        node.handleRollback(new Point(slot(4), hash("a", 4)));
        header(5, 52, hash("c", 5));                       // canonical replacement header
        body(5, 50, hash("o", 5));                         // in-flight orphan body arrives

        assertOrphanRefused();

        assertCanonicalChainStored();
    }

    /** (b) The same in-flight orphan body, delivered BEFORE the canonical replacement header arrives. */
    @Test
    void orphanBodyDeliveredBeforeCanonicalHeader_isRefused_andTheCanonicalChainIsStored() throws Exception {
        header(5, 50, hash("o", 5));
        node.handleRollback(new Point(slot(4), hash("a", 4)));
        body(5, 50, hash("o", 5));                         // in-flight orphan body arrives first

        assertOrphanRefused();

        header(5, 52, hash("c", 5));
        assertCanonicalChainStored();
    }

    /** (c) Same-slot battle (the 2026-11-09 case): the orphan body never becomes the body tip. */
    @Test
    void sameSlotOrphanBody_isNotStored() throws Exception {
        header(5, 50, hash("o", 5));
        node.handleRollback(new Point(slot(4), hash("a", 4)));
        header(5, 50, hash("c", 5));
        body(5, 50, hash("o", 5));

        assertThat(chain.getTip().getBlockNumber()).isEqualTo(4);
        assertThat(HexUtil.encodeHexString(chain.getTip().getBlockHash())).isEqualTo(hash("a", 4));
        assertThat(chain.getBlock(HexUtil.decodeHexString(hash("o", 5)))).isNull();
    }

    /** (d) A normal in-order chain, each header stored before its body, is stored in full: the guard refuses nothing legitimate. */
    @Test
    void inOrderChain_isFullyStored() {
        for (int n = 5; n <= 10; n++) {
            header(n, slot(n), hash("a", n));
            body(n, slot(n), hash("a", n));
        }

        ChainTip tip = chain.getTip();
        assertThat(tip.getBlockNumber()).isEqualTo(10);
        assertThat(tip.getSlot()).isEqualTo(slot(10));
        assertThat(HexUtil.encodeHexString(tip.getBlockHash())).isEqualTo(hash("a", 10));
        for (int n = 1; n <= 10; n++) {
            assertThat(chain.getBlock(HexUtil.decodeHexString(hash("a", n)))).as("body #%d", n).isNotNull();
            assertThat(chain.getSlotByBlockNumber((long) n)).as("slot of #%d", n).isEqualTo(slot(n));
            assertThat(applied).as("BlockApplied for #%d", n).contains(hash("a", n));
        }
        assertThat(applied).hasSize(10);
    }

    private void assertOrphanRefused() throws Exception {
        ChainTip tip = chain.getTip();
        assertThat(tip.getBlockNumber()).as("body tip after the orphan body").isEqualTo(4);
        assertThat(HexUtil.encodeHexString(tip.getBlockHash())).as("body tip hash after the orphan body")
                .isEqualTo(hash("a", 4));
        assertThat(chain.getBlock(HexUtil.decodeHexString(hash("o", 5)))).as("orphan body stored").isNull();
        assertThat(received).as("BlockReceived published for the orphan").doesNotContain(hash("o", 5));
        assertThat(applied).as("BlockApplied published for the orphan").doesNotContain(hash("o", 5));
        assertThat(((AtomicInteger) get(bfm, "consecutiveStaleBlocks")).get())
                .as("an orphan-body refusal is not a stale block").isZero();
    }

    private void assertCanonicalChainStored() {
        body(5, 52, hash("c", 5));                         // canonical #5 body
        header(6, 60, hash("c", 6));
        body(6, 60, hash("c", 6));

        assertThat(chain.getBlock(HexUtil.decodeHexString(hash("c", 5)))).as("canonical #5 body").isNotNull();
        assertThat(chain.getBlock(HexUtil.decodeHexString(hash("c", 6)))).as("canonical #6 body").isNotNull();
        ChainTip tip = chain.getTip();
        assertThat(tip.getBlockNumber()).as("body tip").isEqualTo(6);
        assertThat(HexUtil.encodeHexString(tip.getBlockHash())).as("body tip hash").isEqualTo(hash("c", 6));
        assertThat(chain.getSlotByBlockNumber(5L)).as("slot of canonical #5").isEqualTo(52L);
    }

    private static long slot(int n) { return n * 10L; }

    private static String hash(String fork, int n) {
        String p = fork.equals("a") ? "aa" : fork.equals("o") ? "0f" : "cc";
        return p + String.format("%062x", n);
    }

    private void header(long n, long slot, String hash) {
        chain.storeBlockHeader(HexUtil.decodeHexString(hash), n, slot, new byte[]{(byte) n});
    }

    private void body(long n, long slot, String hash) {
        Block b = Block.builder().era(Era.Conway).cbor("82" + String.format("%02x", n))
                .header(BlockHeader.builder().headerBody(HeaderBody.builder()
                        .blockNumber(n).slot(slot).blockHash(hash).build()).build()).build();
        bfm.onBlock(Era.Conway, b, List.of());
    }

    private void set(String name, Object v) throws Exception { f(YaciNode.class, name).set(node, v); }
    private Object get(String name) throws Exception { return f(YaciNode.class, name).get(node); }
    private static Object get(Object o, String name) throws Exception { return f(o.getClass(), name).get(o); }

    private static Field f(Class<?> c, String name) throws Exception {
        Field fl = c.getDeclaredField(name);
        fl.setAccessible(true);
        return fl;
    }
}
