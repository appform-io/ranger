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
package io.appform.ranger.hub.server.bundle.configuration;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.appform.ranger.http.config.HttpClientConfig;
import io.appform.ranger.hub.server.bundle.models.BackendType;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerHttpUpstreamConfiguration extends RangerUpstreamConfiguration {

    @NotEmpty
    @Valid
    private List<HttpClientConfig> httpClientConfigs;

  public RangerHttpUpstreamConfiguration() {
    super(BackendType.HTTP);
  }

    @Override
    public <T> T accept(RangerConfigurationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
