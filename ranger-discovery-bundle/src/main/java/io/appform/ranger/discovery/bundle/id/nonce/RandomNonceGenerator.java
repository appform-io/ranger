package io.appform.ranger.discovery.bundle.id.nonce;

import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
import dev.failsafe.event.ExecutionAttemptedEvent;
import io.appform.ranger.discovery.bundle.id.CollisionChecker;
import io.appform.ranger.discovery.bundle.id.Constants;
import io.appform.ranger.discovery.bundle.id.Domain;
import io.appform.ranger.discovery.bundle.id.GenerationResult;
import io.appform.ranger.discovery.bundle.id.NonceInfo;
import io.appform.ranger.discovery.bundle.id.IdValidationState;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationInput;
import lombok.val;
import org.joda.time.DateTime;

import javax.inject.Singleton;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

@Singleton
public class RandomNonceGenerator extends NonceGenerator {
    
    private final FailsafeExecutor<GenerationResult> retryer;
    private final SecureRandom secureRandom;
    
    public RandomNonceGenerator() {
        val retryPolicy = RetryPolicy.<GenerationResult>builder()
                .withMaxAttempts(readRetryCount())
                .handleIf(throwable -> true)
                .handleResultIf(Objects::isNull)
                .handleResultIf(generationResult -> generationResult.getState() == IdValidationState.INVALID_RETRYABLE)
                .onRetry(this::retryEventListener)
                .build();
        this.retryer = Failsafe.with(Collections.singletonList(retryPolicy));
        this.secureRandom = new SecureRandom(Long.toBinaryString(System.currentTimeMillis()).getBytes());
    }

    @Override
    public NonceInfo generate(final String namespace) {
        return random(Domain.DEFAULT.getCollisionChecker());
    }

    @Override
    public NonceInfo generateWithConstraints(final IdGenerationInput request) {
        val domain = request.getDomain() != null ? request.getDomain() : Domain.DEFAULT;
        return random(domain.getCollisionChecker());
    }

    @Override
    public void retryEventListener(final ExecutionAttemptedEvent<GenerationResult> event) {
        val result = event.getLastResult();
        if (null != result && !result.getState().equals(IdValidationState.VALID)) {
            val idInfo = result.getNonceInfo();
            val domain = result.getDomain() != null ? result.getDomain() : Domain.DEFAULT;
            val collisionChecker = domain.getCollisionChecker();
            collisionChecker.free(idInfo.getTime(), idInfo.getExponent());
        }
    }
    
    @Override
    public FailsafeExecutor<GenerationResult> getRetryer() {
        return retryer;
    }

    private NonceInfo random(final CollisionChecker collisionChecker) {
        int randomGen;
        long curTimeMs;
        do {
            curTimeMs = System.currentTimeMillis();
            randomGen = secureRandom.nextInt(Constants.MAX_ID_PER_MS);
        } while (!collisionChecker.check(curTimeMs, randomGen));
        return new NonceInfo(randomGen, curTimeMs);
    }

}
