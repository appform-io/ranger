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
package io.appform.ranger.zookeeper.healthservice.monitor.sample;

import io.appform.ranger.core.healthcheck.HealthcheckStatus;
import io.appform.ranger.core.healthservice.monitor.sample.RotationStatusMonitor;
import lombok.val;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;

class RotationStatusMonitorTest {

    final String filePath = "/tmp/rangerRotationFile.html";
    File file = new File(filePath);

    @BeforeEach
    void setUp() throws Exception {
        deleteRotationFile();
    }

    @AfterEach
    void tearDown() throws Exception {
        deleteRotationFile();
    }

    @Test
    void testMonitor() throws Exception {
        deleteRotationFile();
        val rotationStatusMonitor = new RotationStatusMonitor("/tmp/rotationFile.html");
        Assertions.assertEquals(HealthcheckStatus.unhealthy, rotationStatusMonitor.monitor());
    }

    @Test
    void testMonitor2() throws Exception {
        deleteRotationFile();
        if (file.createNewFile()) {
            val rotationStatusMonitor = new RotationStatusMonitor(filePath);
            Assertions.assertEquals(HealthcheckStatus.healthy, rotationStatusMonitor.monitor());
        } else {
            System.out.println("Unable to create file = " + filePath);
            throw new Exception("Unable to create file = " + filePath);
        }
    }

    private void deleteRotationFile() throws Exception {
        if (file.exists() && !file.delete()) {
                System.out.println("Unable to delete file = " + filePath);
                throw new Exception("Unable to delete file = " + filePath);
            }

    }
}