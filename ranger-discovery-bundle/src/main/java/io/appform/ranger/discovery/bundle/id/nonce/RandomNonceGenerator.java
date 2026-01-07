package io.appform.ranger.discovery.bundle.id.nonce;

import io.appform.ranger.discovery.bundle.id.CollisionChecker;
import io.appform.ranger.discovery.bundle.id.Constants;
import io.appform.ranger.discovery.bundle.id.Domain;
import io.appform.ranger.discovery.bundle.id.NonceInfo;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationInput;
import lombok.val;

import java.security.SecureRandom;

public class RandomNonceGenerator extends NonceGenerator {
    
    private final SecureRandom secureRandom;
    
    public RandomNonceGenerator() {
        this.secureRandom = new SecureRandom(Long.toBinaryString(System.currentTimeMillis()).getBytes());
    }

    @Override
    public NonceInfo generateWithConstraints(final IdGenerationInput request) {
        val domain = request.getDomain() != null ? request.getDomain() : Domain.DEFAULT;
        return random(domain.getCollisionChecker());
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
