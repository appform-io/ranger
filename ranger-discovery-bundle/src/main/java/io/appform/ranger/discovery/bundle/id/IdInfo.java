package io.appform.ranger.discovery.bundle.id;

import lombok.Value;

@Value
class IdInfo {
    int exponent;
    long time;

    public IdInfo(int exponent, long time) {
        this.exponent = exponent;
        this.time = time;
    }
}
