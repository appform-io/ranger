package io.appform.ranger.discovery.bundle.id;

import io.appform.ranger.discovery.bundle.id.formatter.IdParsers;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.temporal.ChronoField;

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

        var dateTime = parsedId.getGeneratedDate();
        assertEquals(2024, dateTime.get(ChronoField.YEAR_OF_ERA));
        assertEquals(7, dateTime.getMonth().getValue());
        assertEquals(10, dateTime.getDayOfMonth());
        assertEquals(12, dateTime.getHour());
        assertEquals(32, dateTime.getMinute());
        assertEquals(33, dateTime.getSecond());
        assertEquals(616, dateTime.get(ChronoField.MILLI_OF_SECOND));
    }
}
