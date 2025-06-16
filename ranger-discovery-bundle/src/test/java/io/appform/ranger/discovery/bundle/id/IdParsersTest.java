package io.appform.ranger.discovery.bundle.id;

import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.formatter.IdParsers;
import io.appform.ranger.discovery.bundle.id.generator.DefaultIdGenerator;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

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
    void testParseSuccessAfterGenerationWithSuffix() {
        val idGenerator = new DefaultIdGenerator(IdFormatters.suffix());
        val prefix = "TEST";
        val suffix = "007";
        val generatedId = idGenerator.generate(prefix, suffix);
        val parsedId = IdGenerator.parse(generatedId.getId()).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(prefix, parsedId.getPrefix());
        Assertions.assertEquals(suffix, parsedId.getSuffix());
        Assertions.assertEquals(parsedId.getId(), generatedId.getId());
        Assertions.assertEquals(parsedId.getExponent(), generatedId.getExponent());
        Assertions.assertEquals(parsedId.getNode(), generatedId.getNode());
        Assertions.assertEquals(parsedId.getGeneratedDate(), generatedId.getGeneratedDate());
    }

    private void assertDate(final String dateString, final Date date) throws ParseException {
        Assertions.assertEquals(new SimpleDateFormat("yyMMddHHmmssSSS").parse(dateString), date);
    }
}
