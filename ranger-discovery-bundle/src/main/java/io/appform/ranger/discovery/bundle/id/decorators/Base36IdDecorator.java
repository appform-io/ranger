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

import lombok.val;

import java.math.BigInteger;
import java.util.Optional;
import java.util.regex.Pattern;

public class Base36IdDecorator implements IdDecorator {
    private static final Pattern PATTERN = Pattern.compile("([A-Z0-9]{16})(.*)");
    private static final int BASE36_MAX_LENGTH = 16;
    // 15-digit timestamp + 4-digit nodeId + 3-digit nonce = 22 digits
    private static final int BASE10_PAYLOAD_LENGTH = 22;
    
    @Override
    public String decorate(final String idString) {
        return toBase36(idString);
    }
    
    /**
     * Decorates an identifier using the BASE_36 scheme.
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
        val base36Data = matcher.group(1);
        val base10Data = toBase10(base36Data);
        
        return Optional.of(String.format("%s%s", base10Data, matcher.group(2)));
    }
    
    /**
     * Converts a base-10 numeric string to a zero-padded, uppercased base-36 string of length {@link #BASE36_MAX_LENGTH}.
     * Leading zeros are added to ensure a fixed-width output, making the encoded ID length predictable
     * regardless of the magnitude of the input value.
     *
     * @param payload base-10 numeric string representing the raw ID payload
     * @return base-36 encoded string, left-padded with '0's to exactly {@link #BASE36_MAX_LENGTH} characters
     */
    private static String toBase36(final String payload) {
        val base36IdStr = new BigInteger(payload).toString(36).toUpperCase();
        return "0".repeat(Math.max(0, BASE36_MAX_LENGTH - base36IdStr.length())) + base36IdStr;
    }

    /**
     * Converts a base-36 encoded string back to a zero-padded base-10 string of length {@link #BASE10_PAYLOAD_LENGTH}.
     * <p>
     * Base conversion may drop leading zeros present in the original numeric payload. Zero-padding to
     * {@link #BASE10_PAYLOAD_LENGTH} restores the fixed-width format expected by downstream consumers
     * (e.g. {@code yyMMddHHmmssSSS(15) + nodeId(4) + nonce(3) = 22 digits}).
     *
     * @param payload base-36 encoded string (16 uppercase alphanumeric characters)
     * @return base-10 string, left-padded with '0's to exactly {@link #BASE10_PAYLOAD_LENGTH} characters
     */
    private static String toBase10(final String payload) {
        val base10IdStr = new BigInteger(payload, 36).toString();
        return "0".repeat(Math.max(0, BASE10_PAYLOAD_LENGTH - base10IdStr.length())) + base10IdStr;
    }
}
