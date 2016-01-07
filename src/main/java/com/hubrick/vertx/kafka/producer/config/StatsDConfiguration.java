/**
 * Copyright (C) 2016 Etaia AS (oss@hubrick.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hubrick.vertx.kafka.producer.config;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public class StatsDConfiguration {

    private final String host;
    private final Integer port;
    private final String prefix;

    public StatsDConfiguration(String host, Integer port, String prefix) {
        this.host = host;
        this.port = port;
        this.prefix = prefix;
    }

    public String getHost() {
        return host;
    }

    public Integer getPort() {
        return port;
    }

    public String getPrefix() {
        return prefix;
    }
}
