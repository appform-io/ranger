package io.appform.ranger.discovery.bundle.id.weighted;

import lombok.val;

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

    public boolean isIdPresentAtIndex(int index) {
        return lastUsableIdx.get() > index;
    }

    public int getId(int index) {
        val arrayIdx = index % capacity;
        return idList.get(arrayIdx);
    }

    public void setId(int id) {
//        Don't set new IDs if the idPool is already full of unused IDs.
        if (lastUsableIdx.get() >= nextUsableIdx.get() + capacity) {
            return;
        }
        val arrayIdx = lastUsableIdx.get() % capacity;
        idList.set(arrayIdx, id);
        lastUsableIdx.incrementAndGet();
    }

    public int getUsableIdIndex() {
        return nextUsableIdx.getAndIncrement();
    }
}
