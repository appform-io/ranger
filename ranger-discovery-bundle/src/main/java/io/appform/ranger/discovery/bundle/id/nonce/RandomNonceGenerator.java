package io.appform.ranger.discovery.bundle.id.nonce;

import com.google.common.base.Strings;
import dev.failsafe.event.ExecutionAttemptedEvent;
import io.appform.ranger.discovery.bundle.id.CollisionChecker;
import io.appform.ranger.discovery.bundle.id.Constants;
import io.appform.ranger.discovery.bundle.id.Domain;
import io.appform.ranger.discovery.bundle.id.GenerationResult;
import io.appform.ranger.discovery.bundle.id.IdInfo;
import io.appform.ranger.discovery.bundle.id.IdValidationState;
import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationRequest;
import lombok.val;

import java.util.List;
import java.util.Optional;


public class RandomNonceGenerator extends NonceGeneratorBase {

    public RandomNonceGenerator(final IdFormatter idFormatter) {
        super(idFormatter);
    }

    /**
     * Generate id with given namespace
     *
     * @param namespace String namespace for ID to be generated
     * @return Generated IdInfo
     */
    public IdInfo generate(final String namespace) {
        return random(Domain.DEFAULT.getCollisionChecker());
    }

    @Override
    public Optional<IdInfo> generateWithConstraints(final String namespace, final String domain, final boolean skipGlobal) {
        return generateWithConstraints(namespace, getRegisteredDomains().getOrDefault(domain, Domain.DEFAULT), skipGlobal);
    }

    public Optional<IdInfo> generateWithConstraints(final String namespace, final Domain domain, final boolean skipGlobal) {
        return generateWithConstraints(IdGenerationRequest.builder()
                .prefix(namespace)
                .constraints(domain.getConstraints())
                .skipGlobal(skipGlobal)
                .domain(domain.getDomain())
                .idFormatter(domain.getIdFormatter())
                .build());
    }

    public Optional<IdInfo> generateWithConstraints(final IdGenerationRequest request) {
        val collisionChecker = !Strings.isNullOrEmpty(request.getDomain())
                ? getRegisteredDomains().getOrDefault(request.getDomain(), Domain.DEFAULT)
                .getCollisionChecker()
                : Domain.DEFAULT.getCollisionChecker();
        return Optional.ofNullable(getRetryer().get(
                () -> {
                    IdInfo idInfo = random(collisionChecker);
                    val id = getIdFromIdInfo(idInfo, request.getPrefix(), request.getIdFormatter());
                    return new GenerationResult(idInfo, validateId(request.getConstraints(), id, request.isSkipGlobal()), request.getDomain());
                }))
                .filter(generationResult -> generationResult.getState() == IdValidationState.VALID)
                .map(GenerationResult::getIdInfo);
    }

    @Override
    public Optional<IdInfo> generateWithConstraints(final String namespace, final List<IdValidationConstraint> inConstraints, final boolean skipGlobal) {
        return Optional.ofNullable(getRetryer().get(
                        () -> {
                            val idInfo = generate(namespace);
                            val id = getIdFromIdInfo(idInfo, namespace, getIdFormatter());
                            return new GenerationResult(idInfo, validateId(inConstraints, id, skipGlobal), Domain.DEFAULT_DOMAIN_NAME);
                        }))
                .filter(generationResult -> generationResult.getState() == IdValidationState.VALID)
                .map(GenerationResult::getIdInfo);
    }

    @Override
    protected int readRetryCount() {
        try {
            val count = Integer.parseInt(System.getenv().getOrDefault("NUM_ID_GENERATION_RETRIES", "512"));
            if (count <= 0) {
                throw new IllegalArgumentException(
                        "Negative number of retries does not make sense. Please set a proper value for " +
                                "NUM_ID_GENERATION_RETRIES");
            }
            return count;
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Please provide a valid positive integer for NUM_ID_GENERATION_RETRIES");
        }
    }

    @Override
    protected void retryEventListener(ExecutionAttemptedEvent<GenerationResult> event) {
        val result = event.getLastResult();
        if (null != result && !result.getState().equals(IdValidationState.VALID)) {
            val idInfo = result.getIdInfo();
            val collisionChecker = Strings.isNullOrEmpty(result.getDomain())
                    ? Domain.DEFAULT.getCollisionChecker()
                    : getRegisteredDomains().getOrDefault(result.getDomain(), Domain.DEFAULT).getCollisionChecker();
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
