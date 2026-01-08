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
import io.appform.ranger.discovery.bundle.id.nonce.NonceGenerators;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationInput;
import lombok.val;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Optional;
import java.util.regex.Pattern;

public class RandomNonceIdFormatter implements IdFormatter {
    private static final Pattern PATTERN = Pattern.compile("([A-Za-z]*)([\\d]{2})([\\d]{15})([\\d]{4})([\\d]{3})(.*)");
    private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormat.forPattern("yyMMddHHmmssSSS");
    
    @Override
    public FormattedId format(final int nodeId,
                              final IdGenerationInput idGenerationInput) {
        val nonceInfo = NonceGenerators.randomNonceGenerator().generateWithConstraints(idGenerationInput);
        val dateTime = new DateTime(nonceInfo.getTime());
        val randomNonce = nonceInfo.getExponent();
        val id = String.format("%s%04d%03d", DATE_TIME_FORMATTER.print(dateTime), nodeId, randomNonce);
        return FormattedId.builder()
                .id(id)
                .dateTime(dateTime)
                .time(nonceInfo.getTime())
                .exponent(nonceInfo.getExponent())
                .build();
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
                .prefix(matcher.group(1))
                .suffix(matcher.group(6))
                .node(Integer.parseInt(matcher.group(4)))
                .exponent(Integer.parseInt(matcher.group(5)))
                .generatedDate(DATE_TIME_FORMATTER.parseDateTime(matcher.group(3)).toDate())
                .build());
    }
}
