package io.appform.ranger.discovery.bundle.id.nonce;

import dev.failsafe.event.ExecutionAttemptedEvent;
import io.appform.ranger.discovery.bundle.id.CollisionChecker;
import io.appform.ranger.discovery.bundle.id.Constants;
import io.appform.ranger.discovery.bundle.id.Domain;
import io.appform.ranger.discovery.bundle.id.GenerationResult;
import io.appform.ranger.discovery.bundle.id.IdInfo;
import io.appform.ranger.discovery.bundle.id.IdValidationState;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationRequest;
import lombok.val;


public class RandomNonceGenerator extends NonceGeneratorBase {

    public RandomNonceGenerator(final IdFormatter idFormatter) {
        super(idFormatter);
    }

    @Override
    public IdInfo generate(final String namespace) {
        return random(Domain.DEFAULT.getCollisionChecker());
    }

    public IdInfo generateWithConstraints(final IdGenerationRequest request) {
        val domain = request.getDomainInstance() != null ? request.getDomainInstance() : Domain.DEFAULT;
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
            val idInfo = result.getIdInfo();
            val domain = result.getDomain() != null ? result.getDomain() : Domain.DEFAULT;
            val collisionChecker = domain.getCollisionChecker();
            collisionChecker.free(idInfo.getTime(), idInfo.getExponent());
        }
    }

    private IdInfo random(final CollisionChecker collisionChecker) {
        int randomGen;
        long time;
        do {
            time = System.currentTimeMillis();
            randomGen = getSecureRandom().nextInt(Constants.MAX_ID_PER_MS);
        } while (!collisionChecker.check(time, randomGen));
        return new IdInfo(randomGen, time);
    }

}
