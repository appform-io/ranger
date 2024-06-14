package io.appform.ranger.discovery.bundle.id.weighted;

import lombok.val;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class IdPool {
//    List of IDs for the specific IdPool
    private final AtomicIntegerArray idList;

//    Pointer to track the index of the next usable ID
    private final AtomicInteger nextUsableIdx = new AtomicInteger();

//    Pointer to track index of last usable ID. Helps to determine which index the next ID should go to.
    private final AtomicInteger lastUsableIdx = new AtomicInteger();

//    Size of the IdPool
    private final int capacity;

    public IdPool(int size) {
        this.capacity = size;
        idList = new AtomicIntegerArray(capacity);
    }

    private int getId(int index) {
        val arrayIdx = index % capacity;
        return idList.get(arrayIdx);
    }

    public synchronized void setId(int id) {
//        Don't set new IDs if the idPool is already full of unused IDs.
        if (lastUsableIdx.get() >= nextUsableIdx.get() + capacity - 1) {
            return;
        }
        val arrayIdx = lastUsableIdx.getAndIncrement() % capacity;
        idList.set(arrayIdx, id);
    }

    public synchronized Optional<Integer> getNextId() {
        if (nextUsableIdx.get() < lastUsableIdx.get()) {
            val id = getId(nextUsableIdx.get());
            nextUsableIdx.getAndIncrement();
            return Optional.of(id);
        } else {
            return Optional.empty();
        }
    }

    public void reset() {
        lastUsableIdx.set(0);
        nextUsableIdx.set(0);
    }
}
