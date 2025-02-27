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
package io.appform.ranger.discovery.bundle.id;

import lombok.experimental.UtilityClass;

/**
 * All constants for this project
 */
@UtilityClass
public class Constants {
    public static final int MAX_ID_PER_MS = 1000;
    public static final int MAX_NUM_NODES = 10000;
    public static final int MAX_IDS_PER_SECOND = 1_000_000;
    public static final int DEFAULT_DATA_STORAGE_TIME_LIMIT_IN_SECONDS = 60;
    public static final int DEFAULT_PARTITION_RETRY_COUNT = 100;
}
