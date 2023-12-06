package io.appform.ranger.common.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.stream.Collectors;


public class ShardInfoTest {
    private static final ObjectMapper mapper = new ObjectMapper();

    private String getResource(String path) {
        val data = ShardInfoTest.class.getClassLoader().getResourceAsStream(path);
        if(null == data) return null;
        return new BufferedReader(
                new InputStreamReader(data))
                .lines()
                .collect(Collectors.joining("\n"));
    }

    @SneakyThrows
    @SuppressWarnings("SameParameterValue")
    private  <T> T getResource(String path, Class<T> klass) {
        val data = getResource(path);
        if(null == data) return null;
        return mapper.readValue(data, klass);
    }

    @Test
    public void testShardInfo(){
        val shardInfo1 = getResource("fixtures/env1.json", ShardInfo.class);
        val shardInfo2 = getResource("fixtures/env2.json", ShardInfo.class);
        Assertions.assertNotNull(shardInfo1);
        Assertions.assertNotNull(shardInfo2);
        Assertions.assertNotEquals(shardInfo1, shardInfo2);
        Arrays.asList(shardInfo1, shardInfo2).forEach(shardInfo -> Assertions.assertEquals("e", shardInfo.getEnvironment()));
        Assertions.assertEquals("r", shardInfo1.getRegion());
        Assertions.assertNull(shardInfo2.getRegion());
        Assertions.assertNotNull(shardInfo1.getTags());
        Assertions.assertNotNull(shardInfo2.getTags());
        Assertions.assertTrue(shardInfo2.getTags().contains("tag1"));
    }
}
