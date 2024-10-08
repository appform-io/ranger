/*
 * Copyright 2024 Authors, Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.appform.ranger.client;

import io.appform.ranger.client.utils.CriteriaUtils;
import io.appform.ranger.core.units.TestNodeData;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.function.Predicate;

class CriteriaUtilsTest {

    private Predicate<TestNodeData> getCriteria(int shardId){
        return testNodeData -> testNodeData.getShardId() == shardId;
    }

    @Test
    void testGetCriteria(){
        val initialCriteria = getCriteria(1);
        val argCriteria = getCriteria(2);
        var mergedCriteria = CriteriaUtils.getCriteria(true, initialCriteria, argCriteria);
        Assertions.assertFalse(mergedCriteria.test(TestNodeData.builder().shardId(1).build()));
        Assertions.assertFalse(mergedCriteria.test(TestNodeData.builder().shardId(2).build()));
        mergedCriteria = CriteriaUtils.getCriteria(false, initialCriteria, argCriteria);
        Assertions.assertFalse(mergedCriteria.test(TestNodeData.builder().shardId(1).build()));
        Assertions.assertTrue(mergedCriteria.test(TestNodeData.builder().shardId(2).build()));
    }
}
