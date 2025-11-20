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
package io.appform.ranger.discovery.bundle.id.v2.formatter;

import io.appform.ranger.discovery.bundle.id.Id;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

@Slf4j
@UtilityClass
public class IdParsers {
    private static final int DATE_ID_LENGTH = 22;
    private static final Pattern DEFAULT_PATTERN = Pattern.compile("([A-Za-z]*)([0-9]{22})(.*)");
    private static final Pattern PATTERN = Pattern.compile("([A-Za-z]*)([0-9]{2})(.*)");
    
    private final Map<Integer, IdFormatter> parserRegistry = Map.of(
            IdFormatters.original().getType().getValue(), IdFormatters.original(),
            IdFormatters.suffixed().getType().getValue(), IdFormatters.suffixed(),
            IdFormatters.base36Suffixed().getType().getValue(), IdFormatters.base36Suffixed()
    );
    
    /**
     * Parses a string representation of an ID and converts it into an {@link Id} object.
     *
     * <p>Method logic:</p>
     * <ol>
     *   <li>If {@code idString} is {@code null} returns {@link Optional#empty()}.</li>
     *   <li>Try to match {@code DEFAULT_PATTERN} (expects a 22-digit numeric core). If it matches and
     *       the trailing group is empty, treat the matched 22-digit value as a legacy date-style ID:
     *       <ul>
     *         <li>If the matched date length equals {@value #DATE_ID_LENGTH}, delegate parsing to
     *             {@code io.appform.ranger.discovery.bundle.id.formatter.IdParsers.parse(idString)} and
     *             return its result.</li>
     *         <li>Otherwise return {@link Optional#empty()}.</li>
     *       </ul>
     *   </li>
     *   <li>If not a legacy date-style ID, try to match {@code PATTERN} to extract a two-digit parser
     *       type identifier from group(2). If no match or group(2) is empty, return
     *       {@link Optional#empty()}.</li>
     *   <li>Convert the extracted two-digit parser type to an integer, lookup the corresponding
     *       {@link IdFormatter} from the internal registry and delegate parsing to it. The formatter's
     *       {@code parse} result is returned.</li>
     *   <li>Any exception during matching, conversion, lookup or delegated parsing is caught, a warning
     *       is logged and {@link Optional#empty()} is returned.</li>
     * </ol>
     *
     * <p>Notes:</p>
     * <ul>
     *   <li>The method supports legacy 22-digit date IDs (delegated) and formatter-specific IDs
     *       identified by a two-digit numeric type.</li>
     *   <li>If a formatter is not found in the registry or a parsing error occurs the method will
     *       return {@link Optional#empty()} after logging a warning.</li>
     * </ul>
     *
     * @param idString the string representation of the ID to parse; may be {@code null}
     * @return an {@link Optional} containing the parsed {@link Id} or {@link Optional#empty()} when
     * parsing isn't possible or an error occurs
     * @see Id
     * @see IdFormatter
     * @see IdFormatters#original()
     * @see IdFormatters#suffixed()
     * @see IdFormatters#base36Suffixed()
     * @since 1.0
     * @since 1.0
     */
    public Optional<Id> parse(final String idString) {
        if (idString == null) {
            return Optional.empty();
        }
        try {
            val defaultMatcher = DEFAULT_PATTERN.matcher(idString);
            if (defaultMatcher.find() && defaultMatcher.group(3).isEmpty()) {
                val date = defaultMatcher.group(2);
                if (date.length() == DATE_ID_LENGTH) {
                    return io.appform.ranger.discovery.bundle.id.formatter.IdParsers.parse(idString);
                }
                return Optional.empty();
            }
            
            val matcher = PATTERN.matcher(idString);
            if (!matcher.find() || matcher.group(2).isEmpty()) {
                return Optional.empty();
            }
            return parserRegistry.get(Integer.parseInt(matcher.group(2))).parse(idString);
        } catch (Exception e) {
            log.warn("Could not parse idString {}", e.getMessage());
            return Optional.empty();
        }
    }
}
