package io.appform.ranger.discovery.bundle.id;

import io.appform.ranger.discovery.bundle.id.formatter.IdParsers;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class IdParsersTest {

    @Test
    void testDefaultId() throws ParseException {
        val id = "T2407101232336168748798";
        val parsedId = IdParsers.parse(id).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(id, parsedId.getId());
        Assertions.assertEquals(798, parsedId.getExponent());
        Assertions.assertEquals(8748, parsedId.getNode());
        assertDate("240710123233616", parsedId.getGeneratedDate());
    }

    private void assertDate(final String dateString, final ZonedDateTime date) {
        Assertions.assertEquals(DateTimeFormatter.ofPattern("yyMMddHHmmssSSS").parse(dateString), date);
    }
}
