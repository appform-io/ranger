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
package io.appform.ranger.discovery.bundle.id.decorators;

import io.appform.ranger.discovery.bundle.id.Id;
import lombok.val;

import java.math.BigInteger;
import java.util.Optional;
import java.util.regex.Pattern;

public class Base36IdDecorator implements IdDecorator {
    private static final Pattern PATTERN = Pattern.compile("([A-Za-z]*)([\\d]{2})([A-Z0-9]{16})(.*)");
    private static final Integer BASE36_MAX_LENGTH = 16;
    
    @Override
    public String format(final String idString) {
        return toBase36(idString);
    }
    
    /**
     * Formats an identifier using the BASE_36 scheme.
     * <p>
     * The resulting id has the structure:
     * {base36(padded to 16 chars of [yyMMddHHmmssSSS(15 digits) + nodeId(4 digits) + randomNonce(3 digits)])}
     * <p>
     *
     * @return the formatted identifier string
     */
    @Override
    public Optional<String> parse(final String idString) {
        val matcher = PATTERN.matcher(idString);
        if (!matcher.find()) {
            return Optional.empty();
        }
        val base36Data = matcher.group(3);
        val base10Data = toBase10(base36Data);
        
        return Optional.of(String.format("%s%s%s%s", matcher.group(1), matcher.group(2), base10Data, matcher.group(4)));
    }
    
    private static String toBase36(final String payload) {
        val base36IdStr = new BigInteger(payload).toString(36).toUpperCase();
        return "0".repeat(Math.max(0, BASE36_MAX_LENGTH - base36IdStr.length())) + base36IdStr;
    }
    
    private static String toBase10(final String payload) {
        return new BigInteger(payload, 36).toString();
    }
}
