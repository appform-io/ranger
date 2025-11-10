/*
 * Copyright 2025 Authors, Flipkart Internet Pvt. Ltd.
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
package io.appform.ranger.id.nonce;

import dev.failsafe.event.ExecutionAttemptedEvent;
import io.appform.ranger.id.GenerationResult;
import io.appform.ranger.id.NonceInfo;
import io.appform.ranger.id.request.IdGenerationInput;
import lombok.val;

public abstract class NonceGenerator {

    protected NonceGenerator() {

    }

    public int readRetryCount() {
        try {
            val count = Integer.parseInt(System.getenv().getOrDefault("NUM_ID_GENERATION_RETRIES", "512"));
            if (count <= 0) {
                throw new IllegalArgumentException(
                        "Negative number of retries does not make sense. Please set a proper value for " +
                                "NUM_ID_GENERATION_RETRIES");
            }
            return count;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Please provide a valid positive integer for NUM_ID_GENERATION_RETRIES");
        }
    }

    /**
     * Generate id with given namespace
     *
     * @param namespace String namespace for ID to be generated
     * @return Generated IdInfo
     */
    public abstract NonceInfo generate(final String namespace);

    public abstract NonceInfo generateWithConstraints(final IdGenerationInput request);

    public abstract void retryEventListener(final ExecutionAttemptedEvent<GenerationResult> event);

}
