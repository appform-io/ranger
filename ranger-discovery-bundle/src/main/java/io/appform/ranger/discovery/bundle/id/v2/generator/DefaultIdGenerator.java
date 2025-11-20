package io.appform.ranger.discovery.bundle.id.v2.generator;

import io.appform.ranger.discovery.bundle.id.nonce.RandomNonceGenerator;

public class DefaultIdGenerator extends IdGeneratorBase {

    public DefaultIdGenerator() {
        super(new RandomNonceGenerator());
    }
}
