package io.appform.ranger.discovery.bundle.id.formatter;

import lombok.val;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class SecondPrecisionIdFormatter implements IdFormatter {
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyMMddHHmmss");

    @Override
    public String format(final DateTime dateTime,
                         final int nodeId,
                         final int randomNonce) {
        val leftRandom = randomNonce / 1000;
        val rightRandom = randomNonce % 1000;
        return String.format("%s%03d%04d%03d", DATE_TIME_FORMATTER.print(dateTime), leftRandom, nodeId, rightRandom);
    }
}
