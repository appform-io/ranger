package io.appform.ranger.core.util;

import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.core.model.DataStoreType;
import lombok.experimental.UtilityClass;

import java.util.concurrent.atomic.AtomicReference;

import static java.util.concurrent.TimeUnit.*;

@UtilityClass
public class MetricRecorder {

  private static final String PACKAGE_PREFIX = "io.appform.ranger";
  public static final String ACTIVE = "active";
  private static final String INACTIVE = "inactive";
  private static final String NULL_OR_EMPTY_RESPONSE = "nullOrEmptyResponse";
  private static final String DATA_STORE_TYPE = "dataStoreType";
  private static final String DATA_SOURCE = "dataSource";
  private static final String SERVICE_NAME = "serviceName";
  private static final String ZOMBIE_NODES = "zombieNodes";
  private static final String HTTP_CALL = "httpCall";
  private static final String UNKNOWN_FAILURE = "unknownFailure";
  private static final String RESPONSE_PARSE_FAILURE = "responseParseFailure";
  private static final String NODE_DATA_REFRESH = "nodeDataRefresh";
  private static final String HEALTHY = "healthy";
  private static final String UNHEALTHY = "unhealthy";
  private static final String UPDATE = "update";

  public static final String SERVICES_LIST = "services";
  public static final String LIST_NODES = "listNodes";
  public static final String SUCCESS = "success";
  public static final String FAILURE = "failure";
  public static final String NODE_DATA_SINK = "nodeDataSink";
  public static final String REGISTER_SERVICE = "registerService";
  public static final String SERIALIZATION = "serialization";
  public static final String DESERIALIZATION = "deserialization";
  public static final String STALE_DATA_RETAINED = "staleDataRetained";
  public static final String ZK_READ = "zkRead";
  public static final String HEALTH_CHECKER = "healthChecker";
  public static final String NODE_COUNT = "nodeCount";

  private static final AtomicReference<MetricRegistry> metricRegistry = new AtomicReference<>();

  public static void initialize(MetricRegistry registry) {
    metricRegistry.set(registry);
  }

  private static MetricRegistry registry() {
    return metricRegistry.get();
  }

  public static void recordZombieNodeFound(String serviceName) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, ZOMBIE_NODES)).mark();
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, ZOMBIE_NODES, SERVICE_NAME, serviceName)).mark();
    }
  }

  public static void recordNodeDataSourceStatus(DataStoreType dataStoreType, String upstreamId, boolean active) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, active ? ACTIVE : INACTIVE)).mark();
    }
  }

  public static void recordNodeDataRefreshSuccess(DataStoreType dataStoreType, String upstreamId, long elapsed) {
    if (registry() != null) {
      registry().timer(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, NODE_DATA_REFRESH, SUCCESS)).update(elapsed, MILLISECONDS);
    }
  }

  public static void recordNodeDataRefreshFailure(String serviceName, DataStoreType dataStoreType,
                                                  String upstreamId, long elapsed) {
    if (registry() != null) {
      registry().timer(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, NODE_DATA_REFRESH, FAILURE)).update(elapsed, MILLISECONDS);
      registry().timer(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
                      DATA_SOURCE, upstreamId, SERVICE_NAME, serviceName, NODE_DATA_REFRESH, FAILURE))
              .update(elapsed, MILLISECONDS);
    }
  }

  public static void recordStaleDataRetained(String serviceName, DataStoreType dataStoreType, String upstreamId, int size) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, STALE_DATA_RETAINED)).mark();
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, SERVICE_NAME, serviceName, STALE_DATA_RETAINED)).mark();
      registry().histogram(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, SERVICE_NAME, serviceName, STALE_DATA_RETAINED, NODE_COUNT)).update(size);
    }
  }

  public static void recordNodeDataSinkUpdateStatus(DataStoreType dataStoreType, String upstreamId, String status) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, NODE_DATA_SINK, UPDATE, status)).mark();
    }
  }

  public static void recordHealthcheckFailure() {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, HEALTH_CHECKER, FAILURE)).mark();
    }
  }

  public static void recordHealthcheckStatus(boolean healthy) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, HEALTH_CHECKER,"status", healthy ? HEALTHY : UNHEALTHY)).mark();
    }
  }

  public static void recordServicesFetchStatus(DataStoreType dataStoreType, String upstreamId, String success) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, SERVICES_LIST, "fetch", success)).mark();
    }
  }

  public static void recordRemoteCallStatusCode(DataStoreType dataStoreType, String upstreamId,
                                                String remoteCall, int statusCode) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, HTTP_CALL, remoteCall, "responseStatus", Integer.toString(statusCode))).mark();
    }
  }

  public static void recordCacheUpdateOnDroveEvent(DataStoreType dataStoreType, String upstreamId,
                                                   String eventName, String serviceName) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, "cacheUpdateOnDroveEvent", eventName, SERVICE_NAME, serviceName)).mark();
    }
  }

  public static void recordServicesParseFailure(DataStoreType dataStoreType, String upstreamId) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, HTTP_CALL, SERVICES_LIST, RESPONSE_PARSE_FAILURE)).mark();
    }
  }

  public static void recordListNodesParseFailure(DataStoreType dataStoreType, String upstreamId) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, HTTP_CALL, LIST_NODES, RESPONSE_PARSE_FAILURE)).mark();
    }
  }

  public static void recordListNodesParseFailure(DataStoreType dataStoreType, String upstreamId, String serviceName) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, HTTP_CALL, LIST_NODES, SERVICE_NAME, serviceName, RESPONSE_PARSE_FAILURE)).mark();
    }
  }


  public static void recordZookeeperReadUnknownFailure(DataStoreType dataStoreType, String upstreamId,
                                                       String operation, String exceptionName) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, ZK_READ, operation, UNKNOWN_FAILURE)).mark();
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, ZK_READ, operation, UNKNOWN_FAILURE, exceptionName)).mark();
    }
  }

  public static void recordRemoteCallUnknownFailure(DataStoreType dataStoreType, String upstreamId,
                                                    String httpMethodName, String exceptionName) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, HTTP_CALL, httpMethodName, UNKNOWN_FAILURE)).mark();
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, HTTP_CALL, httpMethodName, UNKNOWN_FAILURE, exceptionName)).mark();
    }
  }

  public static void recordRemoteCallUnknownFailure(DataStoreType dataStoreType, String upstreamId,
                                                    String httpMethodName, String exceptionName, String serviceName) {
    recordRemoteCallUnknownFailure(dataStoreType, upstreamId, httpMethodName, exceptionName);
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, HTTP_CALL, httpMethodName, SERVICE_NAME, serviceName,
              UNKNOWN_FAILURE)).mark();
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, HTTP_CALL, httpMethodName, SERVICE_NAME, serviceName,
              UNKNOWN_FAILURE, exceptionName)).mark();
    }
  }

  public static void recordNullOrEmptyServicesListResponse(DataStoreType dataStoreType, String upstreamId) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, HTTP_CALL, SERVICES_LIST, NULL_OR_EMPTY_RESPONSE)).mark();
    }
  }

  public static void recordNullOrEmptyListNodeResponse(DataStoreType dataStoreType, String upstreamId) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, HTTP_CALL, LIST_NODES, NULL_OR_EMPTY_RESPONSE)).mark();
    }
  }

  public static void recordNullOrEmptyListNodeResponse(DataStoreType dataStoreType, String upstreamId,
                                                       String serviceName) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, HTTP_CALL, LIST_NODES, SERVICE_NAME, serviceName, NULL_OR_EMPTY_RESPONSE)).mark();
    }
  }

  public static void recordNodeDataSinkUnknownFailure(DataStoreType dataStoreType, String upstreamId,
                                                      String serviceName, String exceptionName) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, NODE_DATA_SINK, UNKNOWN_FAILURE)).mark();
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, NODE_DATA_SINK, SERVICE_NAME, serviceName, UNKNOWN_FAILURE)).mark();
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, NODE_DATA_SINK, SERVICE_NAME, serviceName, UNKNOWN_FAILURE, exceptionName)).mark();
    }
  }

  public static void recordNodeDataSinkSerDeFailure(DataStoreType dataStoreType, String upstreamId,
                                                    String serDe, String serviceName, String exceptionName) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, NODE_DATA_SINK, serDe, FAILURE)).mark();
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, NODE_DATA_SINK, serDe, SERVICE_NAME, serviceName, FAILURE)).mark();
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, NODE_DATA_SINK, serDe, SERVICE_NAME, serviceName, FAILURE, exceptionName)).mark();
    }
  }

  public static void recordNullOrEmptyRegisterServiceResponse(DataStoreType dataStoreType, String upstreamId,
                                                              String serviceName) {
    if (registry() != null) {
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, HTTP_CALL, REGISTER_SERVICE, NULL_OR_EMPTY_RESPONSE)).mark();
      registry().meter(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
              DATA_SOURCE, upstreamId, HTTP_CALL, REGISTER_SERVICE, SERVICE_NAME, serviceName, NULL_OR_EMPTY_RESPONSE))
              .mark();
    }
  }

  public static void recordServiceNodesReturned(String serviceName, int serviceNodes) {
    if (registry() != null) {
      registry().histogram(MetricRegistry.name(PACKAGE_PREFIX, SERVICE_NAME,
              serviceName, "nodesReturned")).update(serviceNodes);
    }
  }

  public static void recordServicesReturned(int services) {
    if (registry() != null) {
      registry().histogram(MetricRegistry.name(PACKAGE_PREFIX,"servicesReturned"))
              .update(services);
    }
  }

  public static void recordServiceRegistryUpdateNodeCount(String serviceName, DataStoreType dataStoreType, String upstreamId, int size) {
    if (registry() != null) {
      registry().histogram(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
                      DATA_SOURCE, upstreamId, "serviceRegistryUpdate", SERVICE_NAME, serviceName, NODE_COUNT))
              .update(size);
    }
  }

  public static void recordNodesFetchedCount(String serviceName, DataStoreType dataStoreType, String upstreamId, int size) {
    if (registry() != null) {
      registry().histogram(MetricRegistry.name(PACKAGE_PREFIX, DATA_STORE_TYPE, dataStoreType.name(),
                      DATA_SOURCE, upstreamId, LIST_NODES, SERVICE_NAME, serviceName, NODE_COUNT))
              .update(size);
    }
  }
}
