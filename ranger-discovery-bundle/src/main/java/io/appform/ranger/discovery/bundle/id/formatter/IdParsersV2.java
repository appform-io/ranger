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
import io.appform.ranger.discovery.bundle.id.IdGeneratorType;
import io.appform.ranger.discovery.bundle.id.InternalId;
import io.appform.ranger.discovery.bundle.id.decorators.IdDecorator;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

@Slf4j
@UtilityClass
public class IdParsersV2 {
    private static final int DATE_ID_LENGTH = 22;
    private static final Pattern DEFAULT_PATTERN = Pattern.compile("([A-Za-z]*)([\\d]{22})(.*)");
    private static final Pattern PATTERN = Pattern.compile("([A-Za-z]*)([\\d]{2})(.*)");
    
    private final Map<Integer, IdFormatter> formattersParserRegistry = Map.of(
            IdGeneratorType.DEFAULT_V2_RANDOM_NONCE.getValue(), IdGeneratorType.FORMATTER_VALUE_MAP.get(
                    IdGeneratorType.DEFAULT_V2_RANDOM_NONCE.getValue()),
            IdGeneratorType.BASE_36_RANDOM_NONCE.getValue(), IdGeneratorType.FORMATTER_VALUE_MAP.get(
                    IdGeneratorType.BASE_36_RANDOM_NONCE.getValue())
    );
    
    private final Map<Integer, List<IdDecorator>> decoratorsParserRegistry = Map.of(
            IdGeneratorType.DEFAULT_V2_RANDOM_NONCE.getValue(), IdGeneratorType.DECORATOR_REVERSE_VALUE_MAP.get(
                    IdGeneratorType.DEFAULT_V2_RANDOM_NONCE.getValue()),
            IdGeneratorType.BASE_36_RANDOM_NONCE.getValue(), IdGeneratorType.DECORATOR_REVERSE_VALUE_MAP.get(
                    IdGeneratorType.BASE_36_RANDOM_NONCE.getValue())
    );
    
    /**
     * Parses the provided string representation of an identifier into an {@link Id} object.
     * <p>
     * This method attempts to parse the {@code idString} using the following strategy:
     * <ol>
     * <li>Checks if the string matches the {@code DEFAULT_PATTERN}. If matched and valid,
     * it delegates to the default {@code IdParsers}.</li>
     * <li>If the default pattern does not match, it attempts to extract a generator ID
     * (via {@code PATTERN}).</li>
     * <li>Based on the generator ID, it applies a chain of {@code IdDecorator}s to
     * transform the string.</li>
     * <li>Finally, it uses a registered formatter to produce the final {@link Id} object.</li>
     * </ol>
     * <p>
     * If parsing fails at any stage or if an exception occurs, the error is logged,
     * and an empty {@link Optional} is returned.
     *
     * @param idString the string representation of the ID to parse; can be {@code null}.
     * {@code Optional.empty()} if the input is null, invalid, or if parsing fails.
     * @return an {@link Optional} containing the parsed {@link Id} if successful |
       an {@link Optional} containing the parsed {@link Id} or {@link Optional#empty()} when
     * parsing isn't possible or an error occurs
     * @see Id
     * @see IdFormatter
     * @see IdDecorator
     * @see IdFormatters#randomNonce()
     * @see io.appform.ranger.discovery.bundle.id.decorators.IdDecorators#base36()
     * @since 1.0
     */
    public Optional<InternalId> parse(final String idString) {
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
            val generators = Integer.parseInt(matcher.group(2));
            String parsedIdString = idString;
            // Running through decorators
            for (IdDecorator idDecorator: decoratorsParserRegistry.get(generators)) {
                parsedIdString = idDecorator.parse(idString).orElse(null);
            }
            
            if (parsedIdString == null) {
                throw new RuntimeException(String.format("Decorator parsing failed for idString: %s", idString));
            }
            val parsedId = formattersParserRegistry.get(generators).parse(parsedIdString)
                    .orElseThrow(() -> new RuntimeException(String.format("Parsing failed with formatter for idString: %s", idString)));
            parsedId.setId(idString);
            return Optional.of(parsedId);
        } catch (Exception e) {
            log.warn("Could not parse idString {}", e.getMessage());
            return Optional.empty();
        }
    }
}
