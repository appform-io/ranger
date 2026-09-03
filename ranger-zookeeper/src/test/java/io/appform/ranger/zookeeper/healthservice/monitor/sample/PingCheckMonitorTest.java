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

import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import io.appform.ranger.core.healthcheck.HealthcheckStatus;
import io.appform.ranger.core.healthservice.TimeEntity;
import io.appform.ranger.core.healthservice.monitor.sample.PingCheckMonitor;
import lombok.val;
import org.apache.http.client.methods.HttpGet;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

@WireMockTest
class PingCheckMonitorTest {

    @Test
    void testMonitor(WireMockRuntimeInfo wireMockRuntimeInfo) {
        stubFor(get(urlEqualTo("/")).willReturn(aResponse().withStatus(200)));

        val httpRequest = new HttpGet("/");
        val pingCheckMonitor = new PingCheckMonitor(
                new TimeEntity(2, TimeUnit.SECONDS), httpRequest, 5000, 5, 3,
                "localhost", wireMockRuntimeInfo.getHttpPort());
        Assertions.assertEquals(HealthcheckStatus.healthy, pingCheckMonitor.monitor());
        Assertions.assertEquals(HealthcheckStatus.healthy, pingCheckMonitor.monitor());
        Assertions.assertEquals(HealthcheckStatus.healthy, pingCheckMonitor.monitor());
        Assertions.assertEquals(HealthcheckStatus.healthy, pingCheckMonitor.monitor());
        Assertions.assertEquals(HealthcheckStatus.healthy, pingCheckMonitor.monitor());
        Assertions.assertEquals(HealthcheckStatus.healthy, pingCheckMonitor.monitor());
    }

    @Test
    void testMonitor2(WireMockRuntimeInfo wireMockRuntimeInfo) {
        stubFor(get(urlEqualTo("/help")).willReturn(aResponse().withStatus(404)));

        val httpRequest = new HttpGet("/help");
        val pingCheckMonitor = new PingCheckMonitor(
                new TimeEntity(2, TimeUnit.SECONDS), httpRequest, 5000, 5, 3,
                "localhost", wireMockRuntimeInfo.getHttpPort());
        Assertions.assertEquals(HealthcheckStatus.healthy, pingCheckMonitor.monitor());
        Assertions.assertEquals(HealthcheckStatus.healthy, pingCheckMonitor.monitor());
        Assertions.assertEquals(HealthcheckStatus.unhealthy, pingCheckMonitor.monitor());
        Assertions.assertEquals(HealthcheckStatus.unhealthy, pingCheckMonitor.monitor());
    }
}
