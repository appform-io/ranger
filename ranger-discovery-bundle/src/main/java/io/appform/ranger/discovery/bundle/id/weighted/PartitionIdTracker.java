package io.appform.ranger.discovery.bundle.id.weighted;

import java.util.concurrent.atomic.AtomicInteger;

import static io.appform.ranger.discovery.bundle.id.Constants.MAX_IDS_PER_SECOND;

public class PartitionIdTracker {
    // Array to store IdPools for each partition
    private final IdPool[] idPoolList;

    // Counter to keep track of the number of IDs created
    private final AtomicInteger nextIdCounter = new AtomicInteger();

    protected PartitionIdTracker(int partitionSize, int idPoolSize) {
        idPoolList = new IdPool[partitionSize];
        for (int i=0; i<partitionSize; i+= 1){
            idPoolList[i] = new IdPool(idPoolSize);
        }
    }

    // Method to get the IdPool for a specific partition
    public IdPool getPartition(int partitionId) {
        if (partitionId < idPoolList.length) {
            return idPoolList[partitionId];
        }
        throw new IndexOutOfBoundsException("Invalid partitionId " + partitionId + " for IdPool of size" + idPoolList.length);
    }

    public void addId(int partitionId, int id) {
        if (partitionId < idPoolList.length) {
            idPoolList[partitionId].setId(id);
        } else {
            throw new IndexOutOfBoundsException("Invalid partitionId " + partitionId + " for IdPool of size " + idPoolList.length);
        }
    }

    public int getIdCounter() throws Exception {
        if (nextIdCounter.get() < MAX_IDS_PER_SECOND) {
            return nextIdCounter.getAndIncrement();
        } else {
            throw new Exception("ID Generation Per Seconds Limit Reached.");
        }
    }
}
