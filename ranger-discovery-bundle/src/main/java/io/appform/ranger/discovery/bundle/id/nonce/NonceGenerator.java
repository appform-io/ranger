package io.appform.ranger.discovery.bundle.id.nonce;

import dev.failsafe.event.ExecutionAttemptedEvent;
import io.appform.ranger.discovery.bundle.id.GenerationResult;
import io.appform.ranger.discovery.bundle.id.NonceInfo;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationInput;
import lombok.Getter;
import lombok.val;

@Getter
public abstract class NonceGenerator {
    private IdFormatter idFormatter;

    protected NonceGenerator() {

    }

    protected NonceGenerator(final IdFormatter idFormatter1) {
        this.idFormatter = idFormatter1;
    }

    public int readRetryCount() {
        try {
            val count = Integer.parseInt(System.getenv().getOrDefault("NUM_ID_GENERATION_RETRIES", "512"));
            if (count <= 0) {
                throw new IllegalArgumentException(
                        "Negative number of retries does not make sense. Please set a proper value for " +
                                "NUM_ID_GENERATION_RETRIES");
            }
            return count;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Please provide a valid positive integer for NUM_ID_GENERATION_RETRIES");
        }
    }

    /**
     * Generate id with given namespace
     *
     * @param namespace String namespace for ID to be generated
     * @return Generated NonceInfo
     */
    public abstract NonceInfo generate(final String namespace);

    public abstract NonceInfo generateWithConstraints(final IdGenerationInput request);

    public abstract NonceInfo generateForPartition(final String namespace, final int targetPartitionId) ;

    public abstract void retryEventListener(final ExecutionAttemptedEvent<GenerationResult> event);

}
