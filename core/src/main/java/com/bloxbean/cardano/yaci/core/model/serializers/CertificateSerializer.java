package com.bloxbean.cardano.yaci.core.model.serializers;

import co.nstant.in.cbor.model.Array;
import co.nstant.in.cbor.model.DataItem;
import co.nstant.in.cbor.model.UnsignedInteger;
import com.bloxbean.cardano.client.exception.CborRuntimeException;
import com.bloxbean.cardano.yaci.core.model.certs.Certificate;
import com.bloxbean.cardano.yaci.core.model.certs.GenesisKeyDelegation;
import com.bloxbean.cardano.yaci.core.protocol.Serializer;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;

@Slf4j
public enum CertificateSerializer implements Serializer<Certificate> {
    INSTANCE;

    @Override
    public Certificate deserializeDI(DataItem certArrayDI) {
        Array certArray = (Array)certArrayDI;
        Objects.requireNonNull(certArray);

        List<DataItem> dataItemList = certArray.getDataItems();
        if (dataItemList == null || dataItemList.size() < 2) {
            throw new CborRuntimeException("Certificate deserialization failed. Invalid number of DataItem(s) : "
                    + (dataItemList != null ? String.valueOf(dataItemList.size()) : null));
        }

        UnsignedInteger typeUI = (UnsignedInteger) dataItemList.get(0);
        int type = typeUI.getValue().intValue();

        Certificate certificate;
        switch (type) {
            case 0:
                certificate = StakeRegistrationSerializer.INSTANCE.deserializeDI(certArray);
                break;
            case 1:
                certificate = StakeDeregistrationSerializer.INSTANCE.deserializeDI(certArray);
                break;
            case 2:
                certificate = StakeDelegationSerializer.INSTANCE.deserializeDI(certArray);
                break;
            case 3:
                certificate = PoolRegistrationSerializer.INSTANCE.deserializeDI(certArray);
                break;
            case 4:
                //Pool retirement
                certificate = PoolRetirementSerializer.INSTANCE.deserializeDI(certArray);
                break;
            case 5:
                try {
                    //Genesis key delegation
                    certificate = GenesisKeyDelegationSerializer.INSTANCE.deserializeDI(certArray);
                } catch (Exception e) {
                    e.printStackTrace(); //TODO -- Keep this try catch temporary. Remove once the test is done.
                    return new GenesisKeyDelegation(null, null, null);
                }
                break;
            case 6:
                //Move instateneous rewards certs
                certificate = MoveInstantaneousRewardsSerializer.INSTANCE.deserializeDI(certArray);
                break;
            case 7:
            case 8:
            case 9:
            case 10:
            case 11:
            case 12:
            case 13:
            case 14:
            case 15:
            case 16:
            case 17:
            case 18:
                log.warn("Certificate type {} is not supported yet", type);
                certificate = null;
                break;
            default:
                throw new CborRuntimeException("Certificate deserialization failed. Unknown type : " + type);
        }

        return certificate;
    }
}
