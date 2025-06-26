package io.appform.ranger.discovery.bundle.id;

import io.appform.ranger.discovery.bundle.id.formatter.IdParsers;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class IdParsersTest {

    @Test
    void testDefaultId() {
        val id = "T2407101232336168748798";
        val parsedId = IdParsers.parse(id).orElse(null);
        Assertions.assertNotNull(parsedId);
        assertEquals(id, parsedId.getId());
        assertEquals(798, parsedId.getExponent());
        assertEquals(8748, parsedId.getNode());
        assertEquals(1720594953616L, parsedId.getGeneratedDate().toEpochMilli());
    }
}
