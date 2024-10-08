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

package io.appform.ranger.discovery.bundle.rotationstatus;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Current rotation status
 */
public class RotationStatus {
    private final AtomicBoolean inRotation;

    public RotationStatus(boolean initialStatus) {
        inRotation = new AtomicBoolean(initialStatus);
    }

    public void oor() {
        inRotation.set(false);
    }

    public void bir() {
        inRotation.set(true);
    }

    public boolean status() {
        return inRotation.get();
    }
}
