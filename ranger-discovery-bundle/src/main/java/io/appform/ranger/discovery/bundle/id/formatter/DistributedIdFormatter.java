package io.appform.ranger.discovery.bundle.id.formatter;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DistributedIdFormatter implements IdFormatter {

    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyMMddHHmmss");

    @Override
    public String format(final DateTime dateTime,
                         final int nodeId,
                         final int randomNonce) {
        return String.format("%s%04d%06d", DATE_TIME_FORMATTER.print(dateTime), nodeId, randomNonce);
    }
}
