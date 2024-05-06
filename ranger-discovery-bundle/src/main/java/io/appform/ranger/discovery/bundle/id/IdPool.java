package io.appform.ranger.discovery.bundle.id;

import lombok.Getter;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
public class IdPool {
    private final List<Integer> ids = new CopyOnWriteArrayList<>();
    private final AtomicInteger pointer = new AtomicInteger();

    public int getId(int index) {
        return ids.get(index);
    }
}
