package io.appform.ranger.discovery.bundle.id.generator;

import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.nonce.RandomNonceGenerator;

public class DefaultIdGenerator extends IdGeneratorBase {

    public DefaultIdGenerator() {
        super(IdFormatters.original(), new RandomNonceGenerator());
    }

    public DefaultIdGenerator(final IdFormatter idFormatter) {
        super(idFormatter, new RandomNonceGenerator());
    }
}
