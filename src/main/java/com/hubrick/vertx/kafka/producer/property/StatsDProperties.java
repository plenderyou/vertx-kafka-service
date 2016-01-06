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
package com.hubrick.vertx.kafka.producer.property;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public final class StatsDProperties {

    /* Non-instantiable class */
    private StatsDProperties() {}

    public static final String PREFIX = "prefix";
    public static final String HOST = "host";
    public static final String PORT = "port";

    public static final String PREFIX_DEFAULT = "vertx.kafka";
    public static final String HOST_DEFAULT = "localhost";
    public static final int PORT_DEFAULT = 8125;
}
