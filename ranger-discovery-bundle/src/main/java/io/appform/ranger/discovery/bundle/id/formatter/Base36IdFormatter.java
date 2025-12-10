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

import java.math.BigInteger;
import java.util.Optional;
import java.util.regex.Pattern;


public class Base36IdFormatter implements IdFormatter {
    private static final Pattern PATTERN = Pattern.compile("([A-Za-z]*)([0-9]{2})([A-Z0-9]{16})(.*)");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyMMddHHmmssSSS");
    private static final Integer BASE36_MAX_LENGTH = 16;

    @Override
    public IdGenerationFormatters.IdFormatterType getType() {
        return IdGenerationFormatters.IdFormatterType.BASE_36;
    }
    
    @Override
    public String format(final DateTime dateTime,
                         final int nodeId,
                         final int randomNonce,
                         final String suffix,
                         final int idGenerationFormatters) {
        return String.format("%02d%s%s", idGenerationFormatters,
                toBase36(String.format("%s%04d%03d", DATE_TIME_FORMATTER.print(dateTime), nodeId, randomNonce)), suffix);
    }
    
    /**
     * Formats an identifier using the BASE_36_SUFFIXED scheme.
     * <p>
     * The resulting id has the structure:
     * {typeValue(2 digits)}{base36(padded to 16 chars of yyMMddHHmmssSSS + nodeId(4 digits) + randomNonce(3 digits))}{suffix}
     * <p>
     * dateTime the timestamp to encode (formatted with {@code DATE_TIME_FORMATTER})
     * nodeId the node identifier (zero-padded to 4 digits)
     * randomNonce the random nonce (zero-padded to 3 digits)
     * suffix an optional trailing suffix appended after the base36 portion
     *
     * @return the formatted identifier string
     */
    @Override
    public Optional<Id> parse(final String idString) {
        val matcher = PATTERN.matcher(idString);
        if (!matcher.find()) {
            return Optional.empty();
        }
        val base36Data = matcher.group(3);
        val base10Data = toBase10(base36Data);
        
        return Optional.of(Id.builder()
                .id(String.format("%s%s%s%s", matcher.group(1), matcher.group(2), base10Data, matcher.group(4)))
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
