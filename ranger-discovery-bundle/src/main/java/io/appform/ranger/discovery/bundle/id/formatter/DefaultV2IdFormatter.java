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

public class DefaultV2IdFormatter implements IdFormatter {
    private static final Pattern PATTERN = Pattern.compile("([A-Za-z]*)([0-9]{2})([0-9]{15})([0-9]{4})([0-9]{3})(.*)");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyMMddHHmmssSSS");

    @Override
    public IdGenerationFormatters.IdFormatterType getType() {
        return IdGenerationFormatters.IdFormatterType.DEFAULT_V2;
    }

    @Override
    public String format(final DateTime dateTime,
                         final int nodeId,
                         final int randomNonce,
                         final String suffix,
                         final int idGenerationFormatters) {
        return String.format("%02d%s%04d%03d%s", idGenerationFormatters, DATE_TIME_FORMATTER.print(dateTime), nodeId, randomNonce, suffix);
    }
    
    /**
     * Parses the given id string produced by this formatter.
     * <p>
     * Expected format: alphabetic prefix followed by a 15-digit timestamp in the
     * pattern `yyMMddHHmmssSSS`, a 4-digit node id and a 3-digit exponent.
     * Example: `AB2301011234567890001002` (prefix `AB`, timestamp `230101123456789`,
     * node `0001`, exponent `002`).
     *
     * @param idString the id string to parse
     * @return an Optional containing the parsed {@link Id} when parsing succeeds;
     * Optional.empty() if the input does not match the expected pattern
     */
    @Override
    public Optional<Id> parse(final String idString) {
        val matcher = PATTERN.matcher(idString);
        if (!matcher.find()) {
            return Optional.empty();
        }
        
        return Optional.of(Id.builder()
                .id(idString)
                .build());
    }
}
