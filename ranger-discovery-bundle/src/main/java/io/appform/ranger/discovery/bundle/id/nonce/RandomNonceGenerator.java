package io.appform.ranger.discovery.bundle.id.nonce;

import com.google.common.base.Strings;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.RetryPolicy;
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
import org.joda.time.DateTime;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;


public class RandomNonceGenerator extends NonceGeneratorBase {
    private final FailsafeExecutor<GenerationResult> RETRYER;

    public RandomNonceGenerator(final IdFormatter idFormatter) {
        super(idFormatter);
        RetryPolicy<GenerationResult> RETRY_POLICY = RetryPolicy.<GenerationResult>builder()
                .withMaxAttempts(readRetryCount())
                .handleIf(throwable -> true)
                .handleResultIf(Objects::isNull)
                .handleResultIf(generationResult -> generationResult.getState() == IdValidationState.INVALID_RETRYABLE)
                .onRetry(event -> {
                    val res = event.getLastResult();
                    if (null != res && !res.getState().equals(IdValidationState.VALID)) {
                        val idInfo = res.getIdInfo();
                        val collisionChecker = Strings.isNullOrEmpty(res.getDomain())
                                ? Domain.DEFAULT.getCollisionChecker()
                                : getREGISTERED_DOMAINS().get(res.getDomain()).getCollisionChecker();
                        collisionChecker.free(idInfo.getTime(), idInfo.getExponent());
                    }
                })
                .build();
        RETRYER = Failsafe.with(Collections.singletonList(RETRY_POLICY));
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
        return generateWithConstraints(namespace, getREGISTERED_DOMAINS().getOrDefault(domain, Domain.DEFAULT), skipGlobal);
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
                ? getREGISTERED_DOMAINS().getOrDefault(request.getDomain(), Domain.DEFAULT)
                .getCollisionChecker()
                : Domain.DEFAULT.getCollisionChecker();
        return Optional.ofNullable(RETRYER.get(
                () -> {
                    IdInfo idInfo = random(collisionChecker);
                    val id = getIdFromIdInfo(idInfo, request.getPrefix(), request.getIdFormatter());
                    return GenerationResult.builder()
                            .idInfo(idInfo)
                            .state(validateId(request.getConstraints(), id, request.isSkipGlobal()))
                            .domain(null)
                            .build();
                }))
                .filter(generationResult -> generationResult.getState() == IdValidationState.VALID)
                .map(GenerationResult::getIdInfo);
    }

    @Override
    public Optional<IdInfo> generateWithConstraints(final String namespace, final List<IdValidationConstraint> inConstraints, final boolean skipGlobal) {
        return Optional.ofNullable(RETRYER.get(
                        () -> {
                            val idInfo = generate(namespace);
                            val id = getIdFromIdInfo(idInfo, namespace, getIdFormatter());
                            return GenerationResult.builder()
                                    .idInfo(idInfo)
                                    .state(validateId(inConstraints, id, skipGlobal))
                                    .domain(null)
                                    .build();
                        }))
                .filter(generationResult -> generationResult.getState() == IdValidationState.VALID)
                .map(GenerationResult::getIdInfo);
    }

    @Override
    public IdInfo generateForPartition(final String namespace, int targetPartitionId) {
        return generate(namespace);
    }

    private IdInfo random(final CollisionChecker collisionChecker) {
        int randomGen;
        long time;
        do {
            time = System.currentTimeMillis();
            randomGen = getSECURE_RANDOM().nextInt(Constants.MAX_ID_PER_MS);
        } while (!collisionChecker.check(time, randomGen));
        return new IdInfo(randomGen, time);
    }

    private static int readRetryCount() {
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
    public DateTime getDateTimeFromTime(final long time) {
        return new DateTime(time);
    }

}
