package io.appform.ranger.discovery.bundle.id;

import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.nonce.NonceGeneratorBase;
import io.appform.ranger.discovery.bundle.id.nonce.NonceGeneratorType;
import io.appform.ranger.discovery.bundle.id.nonce.PartitionAwareNonceGenerator;
import lombok.experimental.UtilityClass;
import lombok.val;
import org.joda.time.DateTime;

import java.time.Clock;
import java.util.function.Function;

@UtilityClass
public class IdUtils {
    public DateTime getDateTimeFromSeconds(long seconds) {
        // Convert seconds to milliSeconds
        val millis = seconds * 1000L;
        // Get DateTime object from milliSeconds
        return new DateTime(millis);
    }

    public NonceGeneratorBase getNonceGenerator(final NonceGeneratorType nonceGeneratorType,
                                                final IdGeneratorConfig idGeneratorConfig,
                                                final Function<String, Integer> partitionResolverSupplier,
                                                final IdFormatter idFormatter,
                                                final MetricRegistry metricRegistry,
                                                final Clock clock) {
        return new PartitionAwareNonceGenerator(idGeneratorConfig, partitionResolverSupplier, idFormatter, metricRegistry, clock);
    }

    public int readRetryCountFromEnv() {
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
}
