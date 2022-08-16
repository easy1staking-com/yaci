package com.bloxbean.cardano.yaci.core.util;

import com.bloxbean.cardano.client.crypto.Blake2bUtil;
import com.bloxbean.cardano.client.transaction.spec.Transaction;
import com.bloxbean.cardano.client.transaction.util.CborSerializationUtil;
import com.bloxbean.cardano.client.util.HexUtil;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class TxUtil {

    public static String calculateTxHash(byte[] cbor) {
        try {
            Transaction transaction = Transaction.deserialize(cbor);
            String txHash = HexUtil.encodeHexString(
                    Blake2bUtil.blake2bHash256(CborSerializationUtil.serialize(transaction.getBody().serialize())));

            return txHash;
        } catch (Exception e) {
            log.error("Unable to calculate transaction hash", e);
            return null;
        }
    }
}
