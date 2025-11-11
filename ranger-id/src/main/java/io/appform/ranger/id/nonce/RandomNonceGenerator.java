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
import io.appform.ranger.id.CollisionChecker;
import io.appform.ranger.id.Constants;
import io.appform.ranger.id.Domain;
import io.appform.ranger.id.GenerationResult;
import io.appform.ranger.id.IdValidationState;
import io.appform.ranger.id.NonceInfo;
import io.appform.ranger.id.request.IdGenerationInput;
import lombok.val;

import java.security.SecureRandom;


public class RandomNonceGenerator extends NonceGenerator {

    private final SecureRandom secureRandom = new SecureRandom(Long.toBinaryString(System.currentTimeMillis()).getBytes());

    @Override
    public NonceInfo generate(final String namespace) {
        return random(Domain.DEFAULT.getCollisionChecker());
    }

    @Override
    public NonceInfo generateWithConstraints(final IdGenerationInput request) {
        val domain = request.getDomain() != null ? request.getDomain() : Domain.DEFAULT;
        return random(domain.getCollisionChecker());
    }

    @Override
    public void retryEventListener(final ExecutionAttemptedEvent<GenerationResult> event) {
        val result = event.getLastResult();
        if (null != result && !result.getState().equals(IdValidationState.VALID)) {
            val idInfo = result.getNonceInfo();
            val domain = result.getDomain() != null ? result.getDomain() : Domain.DEFAULT;
            val collisionChecker = domain.getCollisionChecker();
            collisionChecker.free(idInfo.getTime(), idInfo.getExponent());
        }
    }

    private NonceInfo random(final CollisionChecker collisionChecker) {
        int randomGen;
        long curTimeMs;
        do {
            curTimeMs = System.currentTimeMillis();
            randomGen = secureRandom.nextInt(Constants.MAX_ID_PER_MS);
        } while (!collisionChecker.check(curTimeMs, randomGen));
        return new NonceInfo(randomGen, curTimeMs);
    }

}
