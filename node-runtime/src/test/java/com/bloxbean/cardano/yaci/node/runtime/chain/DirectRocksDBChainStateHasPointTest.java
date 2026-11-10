package com.bloxbean.cardano.yaci.node.runtime.chain;

import com.bloxbean.cardano.yaci.core.protocol.chainsync.messages.Point;
import com.bloxbean.cardano.yaci.core.util.HexUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code hasPoint} as {@code ChainState} documents it: a point counts only while it is on the current chain, and
 * {@code Point.ORIGIN} counts whenever the chain has a tip. The chain-sync server picks a client's rollback point
 * with it. The chain here starts after slot 0, as a node that syncs from a recent point does.
 */
class DirectRocksDBChainStateHasPointTest {

    @TempDir
    Path tempDir;

    private DirectRocksDBChainState chainState;

    @BeforeEach
    void setUp() {
        chainState = new DirectRocksDBChainState(tempDir.resolve("testdb").toString());
    }

    @AfterEach
    void tearDown() {
        if (chainState != null) {
            chainState.close();
        }
    }

    @Test
    void origin_isOnTheChainWheneverThereIsATip() {
        store(1, 1000L, 1);
        assertThat(chainState.getBlockNumberBySlot(0L)).as("no block at slot 0").isNull();

        assertThat(chainState.hasPoint(Point.ORIGIN)).isTrue();
    }

    @Test
    void origin_isNotOnAnEmptyChain() {
        assertThat(chainState.hasPoint(Point.ORIGIN)).isFalse();
    }

    @Test
    void pointOnTheChain_isOnTheChain() {
        store(1, 1000L, 1);
        store(2, 1020L, 2);

        assertThat(chainState.hasPoint(point(1020L, 2))).isTrue();
        assertThat(chainState.hasPoint(point(1000L, 1))).isTrue();
        assertThat(chainState.hasPoint(point(1020L, 9))).as("another block in the same slot").isFalse();
    }

    @Test
    void pointRemovedByARollback_isNotOnTheChain() {
        store(1, 1000L, 1);
        store(2, 1020L, 2);
        store(3, 1050L, 3);

        chainState.rollbackTo(1020L);

        assertThat(chainState.hasPoint(point(1050L, 3))).isFalse();
        assertThat(chainState.hasPoint(point(1020L, 2))).isTrue();

        // The replacement block in the same slot is on the chain; the block it replaced is not
        store(3, 1050L, 4);
        assertThat(chainState.hasPoint(point(1050L, 4))).isTrue();
        assertThat(chainState.hasPoint(point(1050L, 3))).isFalse();
    }

    private void store(long blockNumber, long slot, int hashSuffix) {
        chainState.storeBlockHeader(hash(hashSuffix), blockNumber, slot, new byte[]{(byte) hashSuffix});
        chainState.storeBlock(hash(hashSuffix), blockNumber, slot, new byte[]{(byte) hashSuffix});
    }

    private static Point point(long slot, int hashSuffix) {
        return new Point(slot, HexUtil.encodeHexString(hash(hashSuffix)));
    }

    private static byte[] hash(int suffix) {
        byte[] bytes = new byte[32];
        bytes[31] = (byte) suffix;
        return bytes;
    }
}
