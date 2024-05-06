package io.appform.ranger.discovery.bundle.id;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class PartitionIdTracker {
    private final IdPool[] idList;
    private final int partitionSize;
    private final AtomicInteger counter = new AtomicInteger();

    protected PartitionIdTracker(int partitionSize) {
        this.partitionSize = partitionSize;
        idList = new IdPool[partitionSize];
        for (int i=0; i<partitionSize; i+= 1){
            idList[i] = new IdPool();
        }
    }

    public IdPool getPartition(int partitionId) {
        if (partitionId <= idList.length) {
            return idList[partitionId];
        }
        throw new IndexOutOfBoundsException("Invalid partitionId " + partitionId + " for IdPool of size" + idList.length);
    }
}
