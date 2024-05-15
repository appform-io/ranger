package io.appform.ranger.discovery.bundle.id.weighted;

import lombok.Getter;

import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class PartitionIdTracker {
    // Array to store IdPools for each partition
    private final IdPool[] idPoolList;

    // Size of each partition
    private final int partitionSize;

    // Counter to keep track of the number of IDs created
    private final AtomicInteger nextIdCounter = new AtomicInteger();

    protected PartitionIdTracker(int partitionSize) {
        this.partitionSize = partitionSize;
        idPoolList = new IdPool[partitionSize];
        for (int i=0; i<partitionSize; i+= 1){
            idPoolList[i] = new IdPool();
        }
    }

    // Method to get the IdPool for a specific partition
    public IdPool getPartition(int partitionId) {
        if (partitionId <= idPoolList.length) {
            return idPoolList[partitionId];
        }
        throw new IndexOutOfBoundsException("Invalid partitionId " + partitionId + " for IdPool of size" + idPoolList.length);
    }
}
