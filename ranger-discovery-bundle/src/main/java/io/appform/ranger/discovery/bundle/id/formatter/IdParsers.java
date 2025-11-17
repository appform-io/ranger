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
public class IdParsers {
    private static final int MINIMUM_ID_LENGTH = 22;
    private static final Pattern DEFAULT_PATTERN = Pattern.compile("(.*)([0-9]{22})");
    private static final Pattern PATTERN = Pattern.compile("(.*)([0-9]{22})([0-9]{2})([0-9]{6})");

    private final Map<Integer, IdFormatter> parserRegistry = Map.of(
            IdFormatters.original().getType().getValue(), IdFormatters.original(),
            IdFormatters.suffix().getType().getValue(), IdFormatters.suffix()
    );

    /**
     * Parses a string representation of an ID and converts it into an {@link Id} object.
     *
     * <p>This method attempts to parse the input string using a predefined regex pattern that expects
     * the following format: {@code ([A-Za-z]*)([0-9]{22})([0-9]{2})?(.*)}</p>
     *
     * <p>The parsing process follows these steps:</p>
     * <ol>
     *   <li>Validates that the input string is not null and meets the minimum length requirement (22 characters)</li>
     *   <li>Applies the regex pattern to extract components of the ID</li>
     *   <li>Determines the appropriate formatter based on the parser type (group 3 of the regex)</li>
     *   <li>If no parser type is found, defaults to the original formatter (Sample - T2407101232336168748798)</li>
     *   <li>If an invalid parser type is found, logs a warning and falls back to the original formatter (Sample - 0M00002507241535374297496628)</li>
     * </ol>
     *
     * <p>The method supports different ID formats through a registry of {@link IdFormatter} instances.
     * Currently, the registry contains the original formatter for legacy ID formats.</p>
     *
     * <p><strong>Expected ID Format:</strong></p>
     * <ul>
     *   <li>Prefix: Optional alphabetic characters</li>
     *   <li>Core ID: Exactly 22 numeric digits (required)</li>
     *   <li>Parser Type: Optional 2-digit numeric formatter type identifier</li>
     *   <li>Suffix: Optional additional characters</li>
     * </ul>
     *
     * @param idString the string representation of the ID to parse. Must not be null and should be
     *                 at least {@value #MINIMUM_ID_LENGTH} characters long to be considered valid
     * @return an {@link Optional} containing the parsed {@link Id} if the string could be successfully
     *         parsed and converted, or {@link Optional#empty()} if:
     *         <ul>
     *           <li>The input string is null</li>
     *           <li>The input string is shorter than the minimum required length</li>
     *           <li>The input string doesn't match the expected regex pattern</li>
     *           <li>An exception occurs during parsing</li>
     *         </ul>
     *
     * @see Id
     * @see IdFormatter
     * @see IdFormatters#original()
     * @see IdFormatters#suffix()
     * @since 1.0
     */
    public Optional<Id> parse(final String idString) {
        if (idString == null || idString.length() < MINIMUM_ID_LENGTH) {
            return Optional.empty();
        }
        try {
            val matcher = PATTERN.matcher(idString);
            if (matcher.find()) {
                val parser = parserRegistry.get(Integer.parseInt(matcher.group(3)));
                return parser.parse(idString);
            }
            
            val default_matcher = DEFAULT_PATTERN.matcher(idString);
            if (!default_matcher.find()) {
                return Optional.empty();
            }
            return IdFormatters.original().parse(idString);
        } catch (Exception e) {
            log.warn("Could not parse idString {}", e.getMessage());
            return Optional.empty();
        }
    }

}
