package io.appform.ranger.discovery.bundle.id.generator;

import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.discovery.bundle.id.Id;
import io.appform.ranger.discovery.bundle.id.IdUtils;
import io.appform.ranger.discovery.bundle.id.NonceInfo;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.nonce.NonceGeneratorType;
import io.appform.ranger.discovery.bundle.id.nonce.PartitionAwareNonceGenerator;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.time.Clock;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Pattern;

@Slf4j
public class DistributedIdGenerator extends IdGeneratorBase {
    private static final int MINIMUM_ID_LENGTH = 22;
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyMMddHHmmss");
    private static final Pattern PATTERN = Pattern.compile("(.*)([0-9]{12})([0-9]{3})([0-9]{4})([0-9]{3})");

    public DistributedIdGenerator(final IdGeneratorConfig idGeneratorConfig,
                                  final Function<String, Integer> partitionResolverSupplier,
                                  final NonceGeneratorType nonceGeneratorType,
                                  final MetricRegistry metricRegistry,
                                  final Clock clock,
                                  final IdFormatter idFormatter) {
        super(idFormatter,
                new PartitionAwareNonceGenerator(
                        idGeneratorConfig, partitionResolverSupplier, idFormatter, metricRegistry, clock)
        );
    }

    public DistributedIdGenerator(final IdGeneratorConfig idGeneratorConfig,
                                  final Function<String, Integer> partitionResolverSupplier,
                                  final NonceGeneratorType nonceGeneratorType,
                                  final MetricRegistry metricRegistry,
                                  final Clock clock) {
        this(idGeneratorConfig, partitionResolverSupplier, nonceGeneratorType, metricRegistry, clock, IdFormatters.secondPrecision());
    }

    public Id generateForPartition(final String namespace, final int targetPartitionId) {
        val idInfo = nonceGenerator.generateForPartition(namespace, targetPartitionId);
        return this.getIdFromNonceInfo(idInfo, namespace, idFormatter);
    }

    /**
     * Generate id by parsing given string
     *
     * @param idString String idString
     * @return ID if it could be generated
     */
    public Optional<Id> parse(final String idString) {
        if (idString == null
                || idString.length() < MINIMUM_ID_LENGTH) {
            return Optional.empty();
        }
        try {
            val matcher = PATTERN.matcher(idString);
            if (!matcher.find()) {
                return Optional.empty();
            }
            return Optional.of(Id.builder()
                    .id(idString)
                    .node(Integer.parseInt(matcher.group(4)))
                    .exponent(Integer.parseInt(matcher.group(3))*1000 + Integer.parseInt(matcher.group(5)))
                    .generatedDate(DATE_TIME_FORMATTER.parseDateTime(matcher.group(2)).toDate())
                    .build());
        } catch (Exception e) {
            log.warn("Could not parse idString {}", e.getMessage());
            return Optional.empty();
        }
    }

}
