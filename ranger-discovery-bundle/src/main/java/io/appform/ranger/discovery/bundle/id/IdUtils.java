package io.appform.ranger.discovery.bundle.id;

import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.generator.IdGeneratorBase;
import io.appform.ranger.discovery.bundle.id.nonce.NonceGeneratorBase;
import io.appform.ranger.discovery.bundle.id.nonce.NonceGeneratorType;
import io.appform.ranger.discovery.bundle.id.nonce.PartitionAwareNonceGenerator;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.joda.time.DateTime;

import java.time.Clock;
import java.util.function.Function;

@Slf4j
@UtilityClass
public class IdUtils {
    public NonceGeneratorBase getNonceGenerator(final NonceGeneratorType nonceGeneratorType,
                                                final IdGeneratorConfig idGeneratorConfig,
                                                final Function<String, Integer> partitionResolverSupplier,
                                                final IdFormatter idFormatter,
                                                final MetricRegistry metricRegistry,
                                                final Clock clock) {
        return new PartitionAwareNonceGenerator(idGeneratorConfig, partitionResolverSupplier, idFormatter, metricRegistry, clock);
    }

    public Id getIdFromIdInfo(final IdInfo idInfo, final String namespace, final IdFormatter idFormatter) {
        val dateTime = new DateTime(idInfo.getTime());
        val id = String.format("%s%s", namespace, idFormatter.format(dateTime, IdGeneratorBase.getNODE_ID(), idInfo.getExponent()));
        return Id.builder()
                .id(id)
                .exponent(idInfo.getExponent())
                .generatedDate(dateTime.toDate())
                .node(IdGeneratorBase.getNODE_ID())
                .build();
    }

}
