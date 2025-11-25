package io.appform.ranger.discovery.bundle.id.v2;

import io.appform.ranger.discovery.bundle.id.Id;
import io.appform.ranger.discovery.bundle.id.v2.formatter.IdParsers;
import io.appform.ranger.discovery.bundle.id.v2.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.v2.generator.IdGenerator;
import io.appform.ranger.discovery.bundle.id.v2.generator.IdGeneratorBase;
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
    void testDefaultIdWithType() throws ParseException {
        val id = "T002407101232336168748798";
        val parsedId = IdParsers.parse(id).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(id, parsedId.getId());
        Assertions.assertEquals(798, parsedId.getExponent());
        Assertions.assertEquals(8748, parsedId.getNode());
        assertDate("240710123233616", parsedId.getGeneratedDate());
    }

    @Test
    void testParseSuccessAfterGenerationWithSuffix() {
        val idGenerator = new IdGeneratorBase();
        val prefix = "TEST";
        val suffix = "000007";
        val generatedId = idGenerator.generate(prefix, suffix, IdFormatters.suffixed());
        val parsedId = IdGenerator.parse(generatedId.getId()).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(prefix, parsedId.getPrefix());
        Assertions.assertEquals(suffix, parsedId.getSuffix());
        Assertions.assertEquals(parsedId.getId(), generatedId.getId());
        Assertions.assertEquals(parsedId.getExponent(), generatedId.getExponent());
        Assertions.assertEquals(parsedId.getNode(), generatedId.getNode());
        Assertions.assertEquals(parsedId.getGeneratedDate(), generatedId.getGeneratedDate());
    }

    @Test
    void testParseSuccessAfterGenerationWithConstraintsSuffix() {
        val idGenerator = new IdGeneratorBase();
        val prefix = "TEST";
        val suffix = "000007";
        val domain = "TEST";

        idGenerator.registerDomainSpecificConstraints(domain, Collections.singletonList(id -> true));
        Optional<Id> id = idGenerator.generateWithConstraints(prefix, suffix, domain, IdFormatters.suffixed(), false);

        Assertions.assertTrue(id.isPresent());
        Assertions.assertEquals(34, id.get().getId().length());

        val parsedId = IdGenerator.parse(id.get().getId()).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(prefix, parsedId.getPrefix());
        Assertions.assertEquals(suffix, parsedId.getSuffix());
        Assertions.assertEquals(parsedId.getId(), id.get().getId());
        Assertions.assertEquals(parsedId.getExponent(), id.get().getExponent());
        Assertions.assertEquals(parsedId.getNode(), id.get().getNode());
        Assertions.assertEquals(parsedId.getGeneratedDate(), id.get().getGeneratedDate());
    }
    
    @Test
    void testParseSuccessAfterGenerationWithBase36Suffix() {
        val idGenerator = new IdGeneratorBase();
        val prefix = "TEST";
        val suffix = "007";
        val generatedId = idGenerator.generate(prefix, suffix, IdFormatters.base36Suffixed());
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
