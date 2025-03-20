package io.appform.ranger.discovery.bundle.id.formatter;

/*
 * Copyright 2024 Authors, Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import io.appform.ranger.discovery.bundle.id.Id;
import lombok.val;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.math.BigInteger;
import java.util.Optional;
import java.util.regex.Pattern;

public class Base36SuffixIdFormatter implements IdFormatter {

    private static final Pattern PATTERN = Pattern.compile("([A-Za-z]*)(0)([0-9]{15})([0-9]{2})([0-9]*)");
    private static final Pattern DATE_FORMAT_PATTERN = Pattern.compile("([0-9]{15})([0-9]{4})([0-9]{3})");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyMMddHHmmssSSS");
    private static final Integer BASE36_MAX_LENGTH = 15;

    @Override
    public IdParserType getType() {
        return IdParserType.BASE36_SUFFIX;
    }

    @Override
    public String format(DateTime dateTime, int nodeId, int randomNonce) {
        return String.format("%s%04d%03d%02d", toBase36(DATE_TIME_FORMATTER.print(dateTime)), nodeId, randomNonce, getType().getValue());
    }

    @Override
    public Optional<Id> parse(String idString) {
        val matcher = PATTERN.matcher(idString);
        if (!matcher.find()) {
            return Optional.empty();
        }
        val base36Date = matcher.group(3);
        val base10Date = toBase10(base36Date);

        val dateMatcher = DATE_FORMAT_PATTERN.matcher(base10Date);
        if (!dateMatcher.find()) {
            return Optional.empty();
        }

        return Optional.of(Id.builder()
                .id(idString)
                .prefix(matcher.group(1))
                .suffix(matcher.group(5))
                .node(Integer.parseInt(dateMatcher.group(2)))
                .exponent(Integer.parseInt(dateMatcher.group(3)))
                .generatedDate(DATE_TIME_FORMATTER.parseDateTime(dateMatcher.group(1)).toDate())
                .build());
    }

    private static String toBase36(final String payload) {
        val base36IdStr = new BigInteger(payload).toString(36).toUpperCase();
        return "0".repeat(Math.max(0, BASE36_MAX_LENGTH - base36IdStr.length())) + base36IdStr;
    }

    private static String toBase10(final String payload) {
        return new BigInteger(payload, 36).toString();
    }
}
