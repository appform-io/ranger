package io.appform.ranger.discovery.bundle.id;

import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.formatter.IdParsers;
import io.appform.ranger.discovery.bundle.id.generator.DefaultIdGenerator;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;

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

    @Test
    void testDefaultIdWithNumericPrefix() throws ParseException {
        val id = "0M00002507241535374297496628";
        val parsedId = IdParsers.parse(id).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(id, parsedId.getId());
        Assertions.assertEquals(628, parsedId.getExponent());
        Assertions.assertEquals(7496, parsedId.getNode());
        assertDate("250724153537429", parsedId.getGeneratedDate());
    }

    private void assertDate(final String dateString, final Date date) throws ParseException {
        Assertions.assertEquals(new SimpleDateFormat("yyMMddHHmmssSSS").parse(dateString), date);
    }
}
