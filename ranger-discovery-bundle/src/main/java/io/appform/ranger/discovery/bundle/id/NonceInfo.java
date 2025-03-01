package io.appform.ranger.discovery.bundle.id;

import lombok.Value;

@Value
public class NonceInfo {

    int exponent;
    long time;

    public NonceInfo(int exponent, long time) {
        this.exponent = exponent;
        this.time = time;
    }
}
