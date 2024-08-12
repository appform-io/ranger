package io.appform.ranger.drove.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.phonepe.drove.client.DroveClient;
import com.phonepe.drove.models.api.ApiErrorCode;
import com.phonepe.drove.models.api.ApiResponse;
import com.phonepe.drove.models.api.AppSummary;
import com.phonepe.drove.models.api.ExposedAppInfo;
import com.phonepe.drove.models.application.ApplicationState;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.http.HttpStatus;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 *
 */
@Slf4j
public class DroveApiCommunicator<T> implements DroveCommunicator<T> {
    private final DroveUpstreamConfig config;
    private final DroveClient droveClient;
    private final ObjectMapper mapper;

    public DroveApiCommunicator(
            DroveUpstreamConfig config,
            DroveClient droveClient,
            ObjectMapper mapper) {
        this.config = config;
        this.droveClient = droveClient;
        this.mapper = mapper;
    }

    @Override
    public void close() {
        droveClient.close();
    }

    @Override
    public Optional<String> leader() {
        return droveClient.leader();
    }

    @Override
    public List<String> services() {
        log.debug("Loading services list");
        val appNameTag= Objects.requireNonNullElse(
                config.getDiscoveryTagName(),
                DroveUpstreamConfig.DEFAULT_DISCOVERY_TAG_NAME);
        val skipTagName = Objects.requireNonNullElse(
                config.getSkipTagName(),
                DroveUpstreamConfig.DEFAULT_SKIP_TAG_NAME);
        val url = "/apis/v1/applications";
        return droveClient.execute(
                new DroveClient.Request(DroveClient.Method.GET, url),
                new DroveClient.ResponseHandler<>() {
                    @Override
                    public List<String> defaultValue() {
                        return List.of();
                    }

                    @Override
                    public List<String> handle(DroveClient.Response response) throws Exception {
                        if (response.statusCode() == HttpStatus.SC_OK) {
                            val apiResponse = mapper.readValue(
                                    response.body(),
                                    new TypeReference<ApiResponse<Map<String, AppSummary>>>() {
                                    });
                            if (apiResponse.getStatus().equals(ApiErrorCode.SUCCESS)) {
                                return apiResponse.getData()
                                        .values()
                                        .stream()
                                        .filter(summary -> summary.getState()
                                                .equals(ApplicationState.RUNNING))
                                        .filter(summary -> summary.getTags() == null
                                                || !summary.getTags()
                                                .getOrDefault(skipTagName, "false")
                                                .equals("true"))
                                        .map(appSummary -> {
                                            val providedName = Objects.requireNonNullElseGet(appSummary.getTags(),
                                                                                             Map::<String, String>of)
                                                    .get(appNameTag);
                                            return Strings.isNullOrEmpty(providedName)
                                                    ? appSummary.getName()
                                                   : providedName;
                                        })
                                        .distinct()
                                        .toList();
                            }
                            else {
                                log.error("Error calling drove: " + apiResponse.getMessage());
                            }
                        }
                        return List.of();
                    }
                });
    }

    @Override
    public List<ExposedAppInfo> listNodes(final Service service) {
        log.info("Loading nodes list for service: {}/{}", service.getNamespace(), service.getServiceName());
        val url = String.format("/apis/v1/endpoints/app/%s", service.getServiceName());

        log.debug("Refreshing the node list from url {}", url);
        return droveClient.execute(new DroveClient.Request(DroveClient.Method.GET, url),
                                   new DroveClient.ResponseHandler<>() {
                                       @Override
                                       public List<ExposedAppInfo> defaultValue() {
                                           return List.of();
                                       }

                                       @Override
                                       public List<ExposedAppInfo> handle(DroveClient.Response response) throws Exception {
                                           val apiResponse = mapper.readValue(response.body(),
                                                                              new TypeReference<ApiResponse<List<ExposedAppInfo>>>() {
                                                                              });
                                           if (apiResponse.getStatus().equals(ApiErrorCode.FAILED)) {
                                               log.error("Could not read data from drove. Error: {}",
                                                         apiResponse.getMessage());
                                               return List.of();
                                           }
                                           return Objects.requireNonNullElse(apiResponse.getData(), List.of());
                                       }
                                   });
    }

}
