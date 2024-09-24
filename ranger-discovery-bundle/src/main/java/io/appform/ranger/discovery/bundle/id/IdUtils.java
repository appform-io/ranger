package io.appform.ranger.discovery.bundle.id;

import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.nonce.NonceGeneratorBase;
import io.appform.ranger.discovery.bundle.id.nonce.NonceGeneratorType;
import io.appform.ranger.discovery.bundle.id.nonce.PartitionAwareNonceGenerator;
import lombok.val;
import org.joda.time.DateTime;

import java.time.Clock;
import java.util.function.Function;


public class IdUtils {
    public static DateTime getDateTimeFromSeconds(long seconds) {
        // Convert seconds to milliSeconds
        val millis = seconds * 1000L;
        // Get DateTime object from milliSeconds
        return new DateTime(millis);
    }

    public static NonceGeneratorBase getNonceGenerator(final NonceGeneratorType nonceGeneratorType,
                                                       final IdGeneratorConfig idGeneratorConfig,
                                                       final Function<String, Integer> partitionResolverSupplier,
                                                       final IdFormatter idFormatter,
                                                       final MetricRegistry metricRegistry,
                                                       final Clock clock) {
        return new PartitionAwareNonceGenerator(idGeneratorConfig, partitionResolverSupplier, idFormatter, metricRegistry, clock);
    }
}
