package io.appform.ranger.discovery.bundle.id.nonce;

import dev.failsafe.event.ExecutionAttemptedEvent;
import io.appform.ranger.discovery.bundle.id.Domain;
import io.appform.ranger.discovery.bundle.id.GenerationResult;
import io.appform.ranger.discovery.bundle.id.IdValidationState;
import lombok.experimental.UtilityClass;
import lombok.val;

@UtilityClass
public class NonceUtils {
    
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
    
    public void retryEventListener(final ExecutionAttemptedEvent<GenerationResult> event) {
        val result = event.getLastResult();
        if (null != result && !result.getState().equals(IdValidationState.VALID)) {
            val domain = result.getDomain() != null ? result.getDomain() : Domain.DEFAULT;
            val collisionChecker = domain.getCollisionChecker();
            collisionChecker.free(result.getTime(), result.getExponent());
        }
    }
}
