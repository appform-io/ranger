package io.appform.ranger.discovery.core.id.generator;

import io.appform.ranger.discovery.core.id.formatter.IdFormatters;
import io.appform.ranger.discovery.core.id.nonce.RandomNonceGenerator;

public class DefaultIdGenerator extends IdGeneratorBase {

    public DefaultIdGenerator() {
        super(IdFormatters.original(), new RandomNonceGenerator());
    }
}
