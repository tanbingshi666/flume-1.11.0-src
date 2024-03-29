/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flume.node;

import java.util.HashMap;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.SinkRunner;
import org.apache.flume.SourceRunner;

import com.google.common.collect.ImmutableMap;

public class SimpleMaterializedConfiguration implements MaterializedConfiguration {

    private final Map<String, Channel> channels;
    private final Map<String, SourceRunner> sourceRunners;
    private final Map<String, SinkRunner> sinkRunners;

    public SimpleMaterializedConfiguration() {
        /**
         * 场景驱动情况下(syslogudp->memory->kafka)
         * 缓存 Channel、SourceRunner (封装了 Source)、SinkRunner (封装了 Sink)
         * 1. c1 -> MemoryChannel
         * 2. r1 -> EventDrivenSource,
         * EventDrivenSource 封装了 SyslogUDPSource、SyslogUDP 封装了 ReplicatingChannelSelector、ReplicatingChannelSelector 引用了 MemoryChannel
         * 3. k1 -> SinkRunner
         * SinkRunner 封装了 DefaultSinkProcessor、DefaultSinkProcess 封装了 KafkaSink、KafkaSink 引用了 MemoryChannel
         */
        channels = new HashMap<String, Channel>();
        sourceRunners = new HashMap<String, SourceRunner>();
        sinkRunners = new HashMap<String, SinkRunner>();
    }

    @Override
    public String toString() {
        return "{ sourceRunners:" + sourceRunners + " sinkRunners:" + sinkRunners
                + " channels:" + channels + " }";
    }

    @Override
    public void addSourceRunner(String name, SourceRunner sourceRunner) {
        /**
         * 场景驱动下 r1 -> SourceRunner
         */
        sourceRunners.put(name, sourceRunner);
    }

    @Override
    public void addSinkRunner(String name, SinkRunner sinkRunner) {
        /**
         * 场景驱动下 k1 -> SinkRunner (封装了具体的 Sink)
         */
        sinkRunners.put(name, sinkRunner);
    }

    @Override
    public void addChannel(String name, Channel channel) {
        /**
         * 场景驱动下 c1 -> MemoryChannel
         */
        channels.put(name, channel);
    }

    @Override
    public Map<String, Channel> getChannels() {
        return ImmutableMap.copyOf(channels);
    }

    @Override
    public Map<String, SourceRunner> getSourceRunners() {
        return ImmutableMap.copyOf(sourceRunners);
    }

    @Override
    public Map<String, SinkRunner> getSinkRunners() {
        return ImmutableMap.copyOf(sinkRunners);
    }

}
