package io.appform.ranger.discovery.bundle.id;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import static io.appform.ranger.discovery.bundle.id.Constants.MAX_IDS_PER_SECOND;

/**
 * Tracks generated IDs and pointers for generating next IDs for a partition.
 */
@Slf4j
public class PartitionIdTracker {
    /** Array to store IdPools for each partition */
    private final IdPool[] idPoolList;

    /** Counter to keep track of the number of IDs created */
    private final AtomicInteger nextIdCounter = new AtomicInteger();

    @Getter
    private Instant instant;

    private final Meter generatedIdCountMeter;

    public PartitionIdTracker(final int partitionSize,
                              final int idPoolSize,
                              final Instant instant,
                              final MetricRegistry metricRegistry,
                              final String namespace) {
        this.instant = instant;
        idPoolList = new IdPool[partitionSize];
        for (int i=0; i<partitionSize; i+= 1){
            idPoolList[i] = new IdPool(idPoolSize, metricRegistry, namespace);
        }
        this.generatedIdCountMeter = metricRegistry.meter("generatedIdCount.forPrefix." + namespace);
    }

    /** Method to get the IdPool for a specific partition */
    public IdPool getPartition(final int partitionId) {
        Preconditions.checkArgument(partitionId < idPoolList.length, "Invalid partitionId " + partitionId + " for IdPool of size " + idPoolList.length);
        return idPoolList[partitionId];
    }

    public void addId(final int partitionId, final IdInfo idInfo) {
        Preconditions.checkArgument(partitionId < idPoolList.length, "Invalid partitionId " + partitionId + " for IdPool of size " + idPoolList.length);
        if (instant.getEpochSecond() == idInfo.getTime() / 1000) {
            idPoolList[partitionId].setId(idInfo.getExponent());
        }
    }

    public IdInfo getIdInfo() {
        Preconditions.checkArgument(nextIdCounter.get() < MAX_IDS_PER_SECOND, "ID Generation Per Second Limit Reached.");
        val timeInSeconds = instant.getEpochSecond();
        return new IdInfo(nextIdCounter.getAndIncrement(), timeInSeconds * 1000L);
    }

    public synchronized void reset(final Instant instant) {
        // Do not reset if the time (in seconds) hasn't changed
        if (this.instant.getEpochSecond() == instant.getEpochSecond()) {
            return;
        }
        // Reset all ID Pools because they contain IDs for an expired timestamp
        for (val idPool: idPoolList) {
            idPool.reset();
        }
        generatedIdCountMeter.mark(nextIdCounter.get());
        nextIdCounter.set(0);
        this.instant = instant;
    }
}
