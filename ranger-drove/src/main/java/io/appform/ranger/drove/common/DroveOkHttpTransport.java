package io.appform.ranger.drove.common;

import com.phonepe.drove.client.DroveClient;
import com.phonepe.drove.client.DroveHttpTransport;

import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DroveOkHttpTransport implements DroveHttpTransport {
    @Override
    public <T> T get(URI uri, Map<String, List<String>> headers, DroveClient.ResponseHandler<T> responseHandler) {
        return null;
    }

    @Override
    public <T> T post(
            URI uri,
            Map<String, List<String>> headers,
            String body,
            DroveClient.ResponseHandler<T> responseHandler) {
        return null;
    }

    @Override
    public <T> T put(
            URI uri,
            Map<String, List<String>> headers,
            String body,
            DroveClient.ResponseHandler<T> responseHandler) {
        return null;
    }

    @Override
    public <T> T delete(URI uri, Map<String, List<String>> headers, DroveClient.ResponseHandler<T> responseHandler) {
        return null;
    }

    @Override
    public void close() throws Exception {

    }
}
