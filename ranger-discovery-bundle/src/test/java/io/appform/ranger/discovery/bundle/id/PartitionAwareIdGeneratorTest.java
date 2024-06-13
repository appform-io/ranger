package io.appform.ranger.discovery.bundle.id;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorRetryConfig;
import io.appform.ranger.discovery.bundle.id.constraints.PartitionValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.weighted.PartitionAwareIdGenerator;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Test for {@link PartitionAwareIdGenerator}
 */
@Slf4j
@SuppressWarnings({"unused", "FieldMayBeFinal"})
class PartitionAwareIdGeneratorTest {
    final int numThreads = 5;
    final int iterationCount = 100000;
    final int partitionCount = 1024;
    final Function<String, Integer> partitionResolverSupplier = (txnId) -> Integer.parseInt(txnId.substring(txnId.length() - 6)) % partitionCount;
    final IdGeneratorConfig idGeneratorConfig =
            IdGeneratorConfig.builder()
                    .partitionCount(partitionCount)
                    .idPoolSize(100)
                    .retryConfig(IdGeneratorRetryConfig.builder().idGenerationRetryCount(4096).partitionRetryCount(4096).build())
                    .build();
    private PartitionAwareIdGenerator partitionAwareIdGenerator;

    @BeforeEach
    void setup() {
        val metricRegistry = mock(MetricRegistry.class);
        val meter = mock(Meter.class);
        doReturn(meter).when(metricRegistry).meter(anyString());
        doNothing().when(meter).mark();
        partitionAwareIdGenerator = new PartitionAwareIdGenerator(
                idGeneratorConfig, partitionResolverSupplier, metricRegistry
        );
    }
//    ToDo: Add test for partition distribution spread.

    @Test
    void testGenerateWithBenchmark() throws IOException {
        val allIdsList = Collections.synchronizedList(new ArrayList<String>());
        val totalTime = TestUtil.runMTTest(
                numThreads,
                iterationCount,
                (k) -> {
                    val id = partitionAwareIdGenerator.generate("P");
                    id.ifPresent(value -> allIdsList.add(value.getId()));
                },
                this.getClass().getName() + ".testGenerateWithBenchmark");
        Assertions.assertEquals(numThreads * iterationCount, allIdsList.size());
        checkUniqueIds(allIdsList);
    }

    @Test
    void testGenerateWithConstraints() throws IOException {
        val allIdsList = Collections.synchronizedList(new ArrayList<String>());
        PartitionValidationConstraint partitionConstraint = (k) -> k % 2 == 0;
        partitionAwareIdGenerator.registerGlobalConstraints(partitionConstraint);
        val totalTime = TestUtil.runMTTest(
                numThreads,
                iterationCount,
                (k) -> {
                    val id = partitionAwareIdGenerator.generateWithConstraints("P", (String) null, false);
                    id.ifPresent(value -> allIdsList.add(value.getId()));
                },
                this.getClass().getName() + ".testGenerateWithConstraints");

        Assertions.assertEquals(numThreads * iterationCount, allIdsList.size());
        checkUniqueIds(allIdsList);

//        Assert No ID was generated for Invalid partitions
        for (val id: allIdsList) {
            val partitionId = partitionResolverSupplier.apply(id);
            Assertions.assertTrue(partitionConstraint.isValid(partitionId));
        }
    }

    void checkUniqueIds(List<String> allIdsList) {
        HashSet<String> uniqueIds = new HashSet<>(allIdsList);
        Assertions.assertEquals(allIdsList.size(), uniqueIds.size());
    }

    @Test
    void testUniqueIds() {
        HashSet<String> allIDs = new HashSet<>();
        boolean allIdsUnique = true;
        for (int i = 0; i < iterationCount; i += 1) {
            val txnIdOptional = partitionAwareIdGenerator.generate("P");
            val txnId = txnIdOptional.map(Id::getId).orElse(null);
            if (allIDs.contains(txnId)) {
                log.warn(txnId);
                log.warn(String.valueOf(allIDs));
                allIdsUnique = false;
            } else {
                allIDs.add(txnId);
            }
        }
        Assertions.assertTrue(allIdsUnique);
    }

    @Test
    void testGenerateOriginal() {
        partitionAwareIdGenerator = new PartitionAwareIdGenerator(
                idGeneratorConfig, partitionResolverSupplier, mock(MetricRegistry.class)
        );
        val idOptional = partitionAwareIdGenerator.generate("TEST");
        String id = idOptional.isPresent() ? idOptional.get().getId() : "";
        Assertions.assertEquals(26, id.length());
    }

    @Test
    void testGenerateBase36() {
        partitionAwareIdGenerator = new PartitionAwareIdGenerator(
                idGeneratorConfig,
                (txnId) -> new BigInteger(txnId.substring(txnId.length() - 6), 36).abs().intValue() % partitionCount,
                IdFormatters.base36(),
                mock(MetricRegistry.class)
        );
        val idOptional = partitionAwareIdGenerator.generate("TEST");
        String id = idOptional.isPresent() ? idOptional.get().getId() : "";
        Assertions.assertEquals(18, id.length());
    }

    @Test
    void testConstraintFailure() {
        Assertions.assertFalse(partitionAwareIdGenerator.generateWithConstraints(
                "TST",
                Collections.singletonList((id -> false)),
                true).isPresent());
    }

    @Test
    void testParseFailure() {
        //Null or Empty String
        Assertions.assertFalse(partitionAwareIdGenerator.parse(null).isPresent());
        Assertions.assertFalse(partitionAwareIdGenerator.parse("").isPresent());

        //Invalid length
        Assertions.assertFalse(partitionAwareIdGenerator.parse("TEST").isPresent());

        //Invalid chars
        Assertions.assertFalse(partitionAwareIdGenerator.parse("XCL983dfb1ee0a847cd9e7321fcabc2f223").isPresent());
        Assertions.assertFalse(partitionAwareIdGenerator.parse("XCL98-3df-b1e:e0a847cd9e7321fcabc2f223").isPresent());

        //Invalid month
        Assertions.assertFalse(partitionAwareIdGenerator.parse("ABC2032250959030643972247").isPresent());
        //Invalid date
        Assertions.assertFalse(partitionAwareIdGenerator.parse("ABC2011450959030643972247").isPresent());
        //Invalid hour
        Assertions.assertFalse(partitionAwareIdGenerator.parse("ABC2011259659030643972247").isPresent());
        //Invalid minute
        Assertions.assertFalse(partitionAwareIdGenerator.parse("ABC2011250972030643972247").isPresent());
        //Invalid sec
        Assertions.assertFalse(partitionAwareIdGenerator.parse("ABC2011250959720643972247").isPresent());
    }

    @Test
    void testParseSuccess() {
        val idString = "ABC2011250959030643972247";
        val id = partitionAwareIdGenerator.parse(idString).orElse(null);
        Assertions.assertNotNull(id);
        Assertions.assertEquals(idString, id.getId());
        Assertions.assertEquals(972247, id.getExponent());
        Assertions.assertEquals(643, id.getNode());
        Assertions.assertEquals(TestUtil.generateDate(2020, 11, 25, 9, 59, 3, 0, ZoneId.systemDefault()),
                id.getGeneratedDate());
    }

    @Test
    void testParseSuccessAfterGeneration() {
        val generatedIdOptional = partitionAwareIdGenerator.generate("TEST123");
        val generatedId = generatedIdOptional.orElse(null);
        Assertions.assertNotNull(generatedId);
        val parsedId = partitionAwareIdGenerator.parse(generatedId.getId()).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(parsedId.getId(), generatedId.getId());
        Assertions.assertEquals(parsedId.getExponent(), generatedId.getExponent());
        Assertions.assertEquals(parsedId.getNode(), generatedId.getNode());
    }

}