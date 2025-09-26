package io.appform.ranger.id.generator;

import io.appform.ranger.id.formatter.IdFormatters;
import io.appform.ranger.id.nonce.RandomNonceGenerator;

public class DefaultIdGenerator extends IdGeneratorBase {

    public DefaultIdGenerator() {
        super(IdFormatters.original(), new RandomNonceGenerator());
    }
}
