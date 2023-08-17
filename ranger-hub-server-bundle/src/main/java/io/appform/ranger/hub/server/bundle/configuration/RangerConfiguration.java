/*
 * Copyright 2015 Flipkart Internet Pvt. Ltd.
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
import io.appform.ranger.client.RangerClientConstants;
import io.appform.ranger.hub.server.bundle.models.BackendType;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", visible = true)
@JsonSubTypes({
    @JsonSubTypes.Type(value = RangerHttpConfiguration.class, name = "HTTP"),
    @JsonSubTypes.Type(value = RangerZkConfiguration.class, name = "ZK")
})
@Getter
public abstract class RangerConfiguration {

  @NotNull
  private BackendType type;

  @NotEmpty
  private String namespace;

  @Min(RangerClientConstants.MINIMUM_REFRESH_TIME)
  private int nodeRefreshTimeMs = RangerClientConstants.MINIMUM_REFRESH_TIME;

  protected RangerConfiguration(BackendType type) {
    this.type = type;
  }

}
