package io.appform.ranger.discovery.core.id.nonce;

import dev.failsafe.event.ExecutionAttemptedEvent;
import io.appform.ranger.discovery.core.id.CollisionChecker;
import io.appform.ranger.discovery.core.id.Constants;
import io.appform.ranger.discovery.core.id.Domain;
import io.appform.ranger.discovery.core.id.GenerationResult;
import io.appform.ranger.discovery.core.id.NonceInfo;
import io.appform.ranger.discovery.core.id.IdValidationState;
import io.appform.ranger.discovery.core.id.request.IdGenerationInput;
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
