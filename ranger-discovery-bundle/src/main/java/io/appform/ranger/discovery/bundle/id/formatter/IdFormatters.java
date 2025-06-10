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

import lombok.experimental.UtilityClass;

@UtilityClass
public class IdFormatters {

    private static final IdFormatter originalIdFormatter = new DefaultIdFormatter();
    private static final IdFormatter base36IdFormatter = new Base36IdFormatter(originalIdFormatter);
    private static final IdFormatter suffixIdFormatter = new SuffixIdFormatter();

    public static IdFormatter original() {
        return originalIdFormatter;
    }

    public static IdFormatter base36() {
        return base36IdFormatter;
    }

    public static IdFormatter suffix() {
        return suffixIdFormatter;
    }

}
