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
package io.appform.ranger.discovery.bundle.id.formatter;

import io.appform.ranger.discovery.bundle.id.Id;
import lombok.val;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Optional;
import java.util.regex.Pattern;

public class SuffixIdFormatter implements IdFormatter {
    private static final int TYPE = 10;
    private static final Pattern PATTERN = Pattern.compile("([A-Za-z]*)([0-9]{15})([0-9]{4})([0-9]{3})([0-9]{2})([0-9]*)");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyMMddHHmmssSSS");

    @Override
    public int getType() {
        return TYPE;
    }

    @Override
    public String format(final DateTime dateTime,
                         final int nodeId,
                         final int randomNonce) {
        return String.format("%s%04d%03d%02d", DATE_TIME_FORMATTER.print(dateTime), nodeId, randomNonce, TYPE);
    }

    @Override
    public Optional<Id> parse(final String idString) {
        val matcher = PATTERN.matcher(idString);
        if (!matcher.find()) {
            return Optional.empty();
        }
        return Optional.of(Id.builder()
                .id(idString)
                .prefix(matcher.group(1))
                .suffix(matcher.group(6))
                .node(Integer.parseInt(matcher.group(3)))
                .exponent(Integer.parseInt(matcher.group(4)))
                .generatedDate(DATE_TIME_FORMATTER.parseDateTime(matcher.group(2)).toDate())
                .build());
    }
}
