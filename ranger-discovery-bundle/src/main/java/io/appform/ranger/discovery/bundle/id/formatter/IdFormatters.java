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
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

@Slf4j
@UtilityClass
public class IdFormatters {
    private static final int MINIMUM_ID_LENGTH = 22;
    private static final Pattern PATTERN = Pattern.compile("([A-Za-z]*)([0-9]{22})([0-9]{2})?(.*)");

    private static final IdFormatter originalIdFormatter = new DefaultIdFormatter();
    private static final IdFormatter base36IdFormatter = new Base36IdFormatter(originalIdFormatter);

    private final Map<Integer, IdFormatter> formatterRegistry = Map.of(
            originalIdFormatter.getType(), originalIdFormatter
    );

    public static IdFormatter original() {
        return originalIdFormatter;
    }

    public static IdFormatter base36() {
        return base36IdFormatter;
    }

    /**
     * Generate id by parsing given string
     *
     * @param idString String idString
     * @return ID if it could be generated
     */
    public Optional<Id> parse(final String idString) {
        if (idString == null || idString.length() < MINIMUM_ID_LENGTH) {
            return Optional.empty();
        }
        try {
            val matcher = PATTERN.matcher(idString);
            if (!matcher.find()) {
                return Optional.empty();
            }

            val formatterType = matcher.group(3);

            if (formatterType == null) {
                return originalIdFormatter.parse(idString);
            }

            val formatter = formatterRegistry.get(Integer.parseInt(matcher.group(3)));
            if (formatter == null) {
                return Optional.empty();
            }

            return formatter.parse(idString);
        } catch (Exception e) {
            log.warn("Could not parse idString {}", e.getMessage());
            return Optional.empty();
        }
    }

}
