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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.appform.ranger.core.model.HubConstants;
import io.appform.ranger.hub.server.bundle.models.BackendType;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = RangerHttpUpstreamConfiguration.class, name = "HTTP"),
    @JsonSubTypes.Type(value = RangerZkUpstreamConfiguration.class, name = "ZK"),
    @JsonSubTypes.Type(value = RangerDroveUpstreamConfiguration.class, name = "DROVE"),
})
@Getter
public abstract class RangerUpstreamConfiguration {

  @NotNull
  private BackendType type;

  @Min(HubConstants.MINIMUM_REFRESH_TIME_MS)
  @Max(HubConstants.MAXIMUM_REFRESH_TIME_MS)
  private int nodeRefreshTimeMs = HubConstants.MINIMUM_REFRESH_TIME_MS;

  @Min(HubConstants.MINIMUM_SERVICE_REFRESH_TIMEOUT_MS)
  private int serviceRefreshTimeoutMs = HubConstants.SERVICE_REFRESH_TIMEOUT_MS;

  @Min(HubConstants.MINIMUM_HUB_START_TIMEOUT_MS)
  private int hubStartTimeoutMs = HubConstants.HUB_START_TIMEOUT_MS;

  protected RangerUpstreamConfiguration(BackendType type) {
    this.type = type;
  }

  public abstract <T> T accept(final RangerConfigurationVisitor<T> visitor);
}
