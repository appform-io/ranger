package io.appform.ranger.drove.utils;

import com.phonepe.drove.models.api.ExposedAppInfo;
import io.appform.ranger.core.model.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public interface DroveCommunicator<T> extends AutoCloseable {
    Optional<String> leader();
    List<String> services();
    List<ExposedAppInfo> listNodes(final Service service);

    Map<Service, List<ExposedAppInfo>> listNodes(Iterable<? extends Service> services);
}
