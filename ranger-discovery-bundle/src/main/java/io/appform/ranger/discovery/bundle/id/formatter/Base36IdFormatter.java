/*
 * Copyright (c) 2023 Santanu Sinha <santanu.sinha@gmail.com>
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
 *
 */
package io.appform.ranger.discovery.bundle.id.formatter;

import org.joda.time.DateTime;

import java.math.BigInteger;

public class Base36IdFormatter implements IdFormatter {

    private final IdFormatter idFormatter;

    public Base36IdFormatter(IdFormatter idFormatter) {
        this.idFormatter = idFormatter;
    }

    @Override
    public String format(final DateTime dateTime,
                         final int nodeId,
                         final int randomNonce) {
        return toBase36(idFormatter.format(dateTime, nodeId, randomNonce));
    }

    private static String toBase36(final String payload) {
        return new BigInteger(payload).toString(36).toUpperCase();
    }
}
