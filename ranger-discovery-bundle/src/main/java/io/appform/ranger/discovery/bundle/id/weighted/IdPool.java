package io.appform.ranger.discovery.bundle.id.weighted;

import lombok.Getter;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class IdPool {
//    List of IDs for the specific IdPool
//    ToDo: Check for a better DS
//    ToDo: Check for static sized circular array with additional pointer.
    private final List<Integer> idList = new CopyOnWriteArrayList<>();

//    Pointer to track the index of the next usable ID
    private final AtomicInteger pointer = new AtomicInteger();

    public int getId(int index) {
        return idList.get(index);
    }
}
