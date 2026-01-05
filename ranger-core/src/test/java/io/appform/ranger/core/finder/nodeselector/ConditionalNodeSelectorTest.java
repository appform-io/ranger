package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceNodeSelector;
import io.appform.ranger.core.units.TestNodeData;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ConditionalNodeSelectorTest {

    @Test
    void usesPrimarySelectorWhenConditionIsTrue() {

        final Predicate<List<ServiceNode<TestNodeData>>> condition = nodes -> true;
        final ServiceNodeSelector<TestNodeData> primary = mock(ServiceNodeSelector.class);
        final ServiceNodeSelector<TestNodeData> secondary = mock(ServiceNodeSelector.class);
        final ServiceNode<TestNodeData> serviceNode = ServiceNode.<TestNodeData>builder()
                .host("localhost-1")
                .port(9000)
                .nodeData(TestNodeData.builder().shardId(1).build())
                .build();
        when(primary.select(any())).thenReturn(serviceNode);
        final ConditionalNodeSelector<TestNodeData> selector = new ConditionalNodeSelector(condition, primary,
                                                                                          secondary);

        final ServiceNode<TestNodeData> result = selector.select(List.of());

        verify(primary, times(1)).select(any());
        verify(secondary, never()).select(any());
        assertEquals(serviceNode, result);
    }

    @Test
    void usesSecondarySelectorWhenConditionIsFalse() {

        final Predicate<List<ServiceNode<TestNodeData>>> condition = nodes -> false;
        final ServiceNodeSelector<TestNodeData> primary = mock(ServiceNodeSelector.class);
        final ServiceNodeSelector<TestNodeData> secondary = mock(ServiceNodeSelector.class);
        final ServiceNode<TestNodeData> serviceNode = ServiceNode.<TestNodeData>builder()
                .host("localhost-2")
                .port(9001)
                .nodeData(TestNodeData.builder().shardId(2).build())
                .build();
        when(secondary.select(any())).thenReturn(serviceNode);
        final ConditionalNodeSelector<TestNodeData> selector = new ConditionalNodeSelector(condition, primary,
                                                                                          secondary);

        final ServiceNode<TestNodeData> result = selector.select(List.of());

        verify(secondary, times(1)).select(any());
        verify(primary, never()).select(any());
        assertEquals(serviceNode, result);
    }
}
