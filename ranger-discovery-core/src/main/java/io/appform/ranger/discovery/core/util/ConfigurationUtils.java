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
package io.appform.ranger.discovery.common.util;

import com.google.common.base.Strings;
import io.appform.ranger.discovery.common.Constants;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static io.appform.ranger.discovery.common.Constants.HOST_PORT_DELIMITER;
import static io.appform.ranger.discovery.common.Constants.PATH_DELIMITER;
import static io.appform.ranger.discovery.common.Constants.ZOOKEEPER_HOST_DELIMITER;


@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConfigurationUtils {

    public static String resolveNonEmptyPublishedHost(String publishedHost) throws UnknownHostException {
        if (Strings.isNullOrEmpty(publishedHost) || publishedHost.equals(Constants.DEFAULT_HOST)) {
            return InetAddress.getLocalHost()
                    .getCanonicalHostName();
        }
        return publishedHost;
    }

    public static Set<String> resolveZookeeperHosts(String zkHostString) {
        return Arrays.stream(zkHostString.split(ZOOKEEPER_HOST_DELIMITER))
                .map(zkHostPort -> zkHostPort.split(HOST_PORT_DELIMITER)[0])
                .map(zkHostPath -> zkHostPath.split(PATH_DELIMITER)[0])
                .collect(Collectors.toSet());
    }

}
