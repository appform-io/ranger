/*
 * Copyright 2015 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.appform.ranger.client.http;

import com.google.common.base.Preconditions;
import io.appform.ranger.client.AbstractRangerHubClient;
import io.appform.ranger.core.finder.nodeselector.RandomServiceNodeSelector;
import io.appform.ranger.core.finderhub.ServiceDataSource;
import io.appform.ranger.core.finderhub.ServiceFinderHub;
import io.appform.ranger.core.model.ServiceNodeSelector;
import io.appform.ranger.core.model.ServiceRegistry;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.http.serde.HTTPResponseDataDeserializer;
import io.appform.ranger.http.servicefinderhub.HttpServiceDataSource;
import io.appform.ranger.http.servicefinderhub.HttpServiceFinderHubBuilder;
import io.appform.ranger.http.utils.HttpClientUtils;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.fluent.Executor;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.util.TimeValue;

@Slf4j
@Getter
@SuperBuilder
public abstract class AbstractRangerHttpHubClient<T, R extends ServiceRegistry<T>, D extends HTTPResponseDataDeserializer<T>>
    extends AbstractRangerHubClient<T, R, D> {

  private final HttpClientConfig clientConfig;
  @Builder.Default
  private final ServiceNodeSelector<T> nodeSelector = new RandomServiceNodeSelector<>();
  private CloseableHttpClient httpClient;
  private Executor httpExecutor;

  @Override
  public void start() {
    Preconditions.checkNotNull(clientConfig, "Http Client Config can't be null");
    this.httpClient = HttpClientUtils.getCloseableClient(clientConfig);
    this.httpExecutor = Executor.newInstance(httpClient);
    super.start();
  }

  @Override
  @SneakyThrows
  public void stop() {
    log.info("Stopping the http client");
    if (null != httpClient) {
      httpClient.close();
    }
    super.stop();
  }

  @Override
  protected ServiceDataSource getDefaultDataSource() {
    return new HttpServiceDataSource<>(clientConfig, getMapper(), httpExecutor);
  }

  @Override
  protected ServiceFinderHub<T, R> buildHub() {
    return new HttpServiceFinderHubBuilder<T, R>()
        .withServiceDataSource(getServiceDataSource())
        .withServiceFinderFactory(getFinderFactory())
        .withRefreshFrequencyMs(getNodeRefreshTimeMs())
        .build();
  }
}
