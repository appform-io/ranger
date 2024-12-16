package io.appform.ranger.discovery.bundle.id.formatter;

import io.appform.ranger.discovery.bundle.id.Id;
import lombok.val;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Optional;
import java.util.regex.Pattern;


public class SecondPrecisionIdFormatter implements IdFormatter {
    private static final Pattern PATTERN = Pattern.compile("(.*?)([0-9]{12})([0-9]{3})([0-9]{4})([0-9]{3})([0-9]{2}?)");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyMMddHHmmss");

    @Override
    public String format(final DateTime dateTime,
                         final int nodeId,
                         final int randomNonce) {
        val leftRandom = randomNonce / 1000;
        val rightRandom = randomNonce % 1000;
        return String.format("%s%03d%04d%03d", DATE_TIME_FORMATTER.print(dateTime), leftRandom, nodeId, rightRandom);
    }

    @Override
    public Optional<Id> parse(String idString) {
        val matcher = PATTERN.matcher(idString);
        if (!matcher.find()) {
            return Optional.empty();
        }
        val exponent = (1000 * Integer.parseInt(matcher.group(3))) + Integer.parseInt(matcher.group(5));
        return Optional.of(Id.builder()
                .id(idString)
                .node(Integer.parseInt(matcher.group(4)))
                .exponent(exponent)
                .generatedDate(DATE_TIME_FORMATTER.parseDateTime(matcher.group(2)).toDate())
                .build());
    }
}
