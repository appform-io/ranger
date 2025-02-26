package io.appform.ranger.discovery.bundle.id.nonce;

import dev.failsafe.event.ExecutionAttemptedEvent;
import io.appform.ranger.discovery.bundle.id.CollisionChecker;
import io.appform.ranger.discovery.bundle.id.Constants;
import io.appform.ranger.discovery.bundle.id.Domain;
import io.appform.ranger.discovery.bundle.id.GenerationResult;
import io.appform.ranger.discovery.bundle.id.NonceInfo;
import io.appform.ranger.discovery.bundle.id.IdValidationState;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationInput;
import lombok.val;

import java.security.SecureRandom;


public class RandomNonceGenerator extends NonceGenerator {

    private final SecureRandom secureRandom = new SecureRandom(Long.toBinaryString(System.currentTimeMillis()).getBytes());

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
    public IdInfo generateForPartition(String namespace, int targetPartitionId) {
        throw new UnsupportedOperationException();
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
