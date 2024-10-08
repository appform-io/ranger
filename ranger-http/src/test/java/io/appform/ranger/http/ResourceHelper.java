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
package io.appform.ranger.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.experimental.UtilityClass;
import lombok.val;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

@UtilityClass
public class ResourceHelper {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static String getResource(String path) {
        val data = ResourceHelper.class.getClassLoader().getResourceAsStream(path);
        if(null == data) return null;
        return new BufferedReader(
                new InputStreamReader(data))
                .lines()
                .collect(Collectors.joining("\n"));
    }

    @SneakyThrows
    public static <T> T getResource(String path, Class<T> klass) {
        val data = getResource(path);
        if(null == data) return null;
        return objectMapper.readValue(data, klass);
    }
}
