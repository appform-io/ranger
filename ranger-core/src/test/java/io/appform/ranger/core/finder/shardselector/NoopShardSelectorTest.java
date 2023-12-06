package io.appform.ranger.core.finder.shardselector;

import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.core.utils.RangerTestUtils;
import io.appform.ranger.core.utils.RegistryTestUtils;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class NoopShardSelectorTest {

    @Test
    void testNoOpShardSelector(){
        val serviceRegistry = RegistryTestUtils.getUnshardedRegistry();
        val shardSelector = new NoopShardSelector<TestNodeData>();
        val nodes = shardSelector.nodes(RangerTestUtils.getCriteria(1), serviceRegistry);
        Assertions.assertFalse(nodes.isEmpty());
        Assertions.assertEquals("localhost-1", nodes.get(0).getHost());
    }
}
