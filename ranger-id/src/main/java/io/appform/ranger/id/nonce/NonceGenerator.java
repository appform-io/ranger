package io.appform.ranger.id.nonce;

import io.appform.ranger.id.NonceInfo;
import io.appform.ranger.id.request.IdGenerationInput;

public abstract class NonceGenerator {

    protected NonceGenerator() {

    }

    /**
     * Generate id with given requst
     *
     * @param request Metadata required for ID to be generated
     * @return Generated IdInfo
     */
    public abstract NonceInfo generateWithConstraints(final IdGenerationInput request);
}
