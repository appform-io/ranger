package io.appform.ranger.discovery.bundle.id.generator;

import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.discovery.bundle.id.Id;
import io.appform.ranger.discovery.bundle.id.IdUtils;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.nonce.NonceGeneratorType;
import lombok.val;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.time.Clock;
import java.util.function.Function;
import java.util.regex.Pattern;


public class DistributedIdGenerator extends IdGeneratorBase {
    private static final int MINIMUM_ID_LENGTH = 22;
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyMMddHHmmss");
    private static final Pattern PATTERN = Pattern.compile("(.*)([0-9]{12})([0-9]{4})([0-9]{6})");

    public DistributedIdGenerator(final IdGeneratorConfig idGeneratorConfig,
                                  final Function<String, Integer> partitionResolverSupplier,
                                  final NonceGeneratorType nonceGeneratorType,
                                  final MetricRegistry metricRegistry,
                                  final Clock clock,
                                  final IdFormatter idFormatter) {
        super(MINIMUM_ID_LENGTH,
                DATE_TIME_FORMATTER,
                PATTERN,
                IdUtils.getNonceGenerator(nonceGeneratorType, idGeneratorConfig, partitionResolverSupplier, idFormatter, metricRegistry, clock));
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
        return nonceGenerator.getIdFromIdInfo(idInfo, namespace);
    }

}
