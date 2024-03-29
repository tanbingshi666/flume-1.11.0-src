/**
 Licensed to the Apache Software Foundation (ASF) under one or more
 contributor license agreements.  See the NOTICE file distributed with
 this work for additional information regarding copyright ownership.
 The ASF licenses this file to You under the Apache License, Version 2.0
 (the "License"); you may not use this file except in compliance with
 the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 limitations under the License.
 */

package org.apache.flume.sink.kafka;

public class TestConstants {
  public static final String STATIC_TOPIC = "static-topic";
  public static final String HEADER_TOPIC = "%{header1}-topic";
  public static final String CUSTOM_KEY = "custom-key";
  public static final String CUSTOM_TOPIC = "custom-topic";
  public static final String HEADER_1_VALUE = "test-avro-header";
  public static final String HEADER_1_KEY = "header1";
  public static final String KAFKA_HEADER_1 = "FLUME_CORRELATOR";
  public static final String KAFKA_HEADER_2 = "FLUME_METHOD";
}
