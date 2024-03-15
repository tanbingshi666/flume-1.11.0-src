/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flume.node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.flume.Channel;
import org.apache.flume.ChannelFactory;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.FlumeException;
import org.apache.flume.Sink;
import org.apache.flume.SinkFactory;
import org.apache.flume.SinkProcessor;
import org.apache.flume.SinkRunner;
import org.apache.flume.Source;
import org.apache.flume.SourceFactory;
import org.apache.flume.SourceRunner;
import org.apache.flume.annotations.Disposable;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.ChannelSelectorFactory;
import org.apache.flume.channel.DefaultChannelFactory;
import org.apache.flume.conf.BasicConfigurationConstants;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.conf.Configurables;
import org.apache.flume.conf.FlumeConfiguration;
import org.apache.flume.conf.FlumeConfiguration.AgentConfiguration;
import org.apache.flume.conf.TransactionCapacitySupported;
import org.apache.flume.conf.channel.ChannelSelectorConfiguration;
import org.apache.flume.conf.sink.SinkConfiguration;
import org.apache.flume.conf.sink.SinkGroupConfiguration;
import org.apache.flume.conf.source.SourceConfiguration;
import org.apache.flume.sink.DefaultSinkFactory;
import org.apache.flume.sink.DefaultSinkProcessor;
import org.apache.flume.sink.SinkGroup;
import org.apache.flume.source.DefaultSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("REC_CATCH_EXCEPTION")
public abstract class AbstractConfigurationProvider implements ConfigurationProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractConfigurationProvider.class);

    private final String agentName;
    private final SourceFactory sourceFactory;
    private final SinkFactory sinkFactory;
    private final ChannelFactory channelFactory;

    private final Map<Class<? extends Channel>, Map<String, Channel>> channelCache;

    public AbstractConfigurationProvider(String agentName) {
        super();
        this.agentName = agentName;
        // 创建 source、channel、sink 工厂
        this.sourceFactory = new DefaultSourceFactory();
        this.sinkFactory = new DefaultSinkFactory();
        this.channelFactory = new DefaultChannelFactory();

        channelCache = new HashMap<Class<? extends Channel>, Map<String, Channel>>();
    }

    protected abstract FlumeConfiguration getFlumeConfiguration();

    public MaterializedConfiguration getConfiguration() {
        /**
         * 创建物化配置对象并初始化三个 HashMap
         * 1. channels = new HashMap<String, Channel>()
         * 2. sourceRunners = new HashMap<String, SourceRunner>()
         * 3. sinkRunners = new HashMap<String, SinkRunner>();
         */
        MaterializedConfiguration conf = new SimpleMaterializedConfiguration();

        /**
         * 初步解析配置文件 sources、channels、sinks
         */
        FlumeConfiguration fconfig = getFlumeConfiguration();

        /**
         * 根据入口参数 --name <agent-name> 中的 <agent-name> 获取 AgentConfiguration
         */
        AgentConfiguration agentConf = fconfig.getConfigurationFor(getAgentName());
        if (agentConf != null) {

            Map<String, ChannelComponent> channelComponentMap = Maps.newHashMap();
            Map<String, SourceRunner> sourceRunnerMap = Maps.newHashMap();
            Map<String, SinkRunner> sinkRunnerMap = Maps.newHashMap();

            try {
                /**
                 * 加载 channels 核心根据配置文件 channel 类型创建对应的 Channel
                 * 比如 memory -> MemoryChannel
                 * 结果: channelComponentMap = { c1 -> new ChannelComponent(MemoryChannel) }
                 */
                loadChannels(agentConf, channelComponentMap);

                /**
                 * 初始化具体的 Source、Selector 并连接具体的 Channel
                 * 结果:sourceRunnerMap = { r1 - > EventDrivenSource(SyslogUDPSource) }
                 */
                loadSources(agentConf, channelComponentMap, sourceRunnerMap);

                /**
                 * 初始化具体的 Sink 并连接具体的 Channel
                 * sinkRunnerMap = { k1 -> SinkRunner(DefaultSinkProcessor(KafkaSink)) }
                 */
                loadSinks(agentConf, channelComponentMap, sinkRunnerMap);

                /**
                 * 场景驱动情况下 channelNames = [c1]
                 */
                Set<String> channelNames = new HashSet<String>(channelComponentMap.keySet());
                for (String channelName : channelNames) {
                    ChannelComponent channelComponent = channelComponentMap.get(channelName);
                    if (channelComponent.components.isEmpty()) {
                        LOGGER.warn(String.format("Channel %s has no components connected" +
                                " and has been removed.", channelName));
                        channelComponentMap.remove(channelName);
                        Map<String, Channel> nameChannelMap =
                                channelCache.get(channelComponent.channel.getClass());
                        if (nameChannelMap != null) {
                            nameChannelMap.remove(channelName);
                        }
                    } else {
                        LOGGER.info(String.format("Channel %s connected to %s", channelName, channelComponent.components.toString()));
                        /**
                         * 往 SimpleMaterializedConfiguration 添加 Channel 信息
                         */
                        conf.addChannel(channelName, channelComponent.channel);
                    }
                }
                for (Map.Entry<String, SourceRunner> entry : sourceRunnerMap.entrySet()) {
                    /**
                     * 往 SimpleMaterializedConfiguration 添加 Source 信息
                     */
                    conf.addSourceRunner(entry.getKey(), entry.getValue());
                }
                for (Map.Entry<String, SinkRunner> entry : sinkRunnerMap.entrySet()) {
                    /**
                     * 往 SimpleMaterializedConfiguration 添加 Sink 信息
                     */
                    conf.addSinkRunner(entry.getKey(), entry.getValue());
                }
            } catch (InstantiationException ex) {
                LOGGER.error("Failed to instantiate component", ex);
            } finally {
                channelComponentMap.clear();
                sourceRunnerMap.clear();
                sinkRunnerMap.clear();
            }
        } else {
            LOGGER.warn("No configuration found for this host:{}", getAgentName());
        }
        return conf;
    }

    public String getAgentName() {
        return agentName;
    }

    private void loadChannels(AgentConfiguration agentConf,
                              Map<String, ChannelComponent> channelComponentMap)
            throws InstantiationException {
        LOGGER.info("Creating channels");

        /*
         * Some channels will be reused across re-configurations. To handle this,
         * we store all the names of current channels, perform the reconfiguration,
         * and then if a channel was not used, we delete our reference to it.
         * This supports the scenario where you enable channel "ch0" then remove it
         * and add it back. Without this, channels like memory channel would cause
         * the first instances data to show up in the seconds.
         */
        ListMultimap<Class<? extends Channel>, String> channelsNotReused = ArrayListMultimap.create();
        // assume all channels will not be re-used
        for (Map.Entry<Class<? extends Channel>, Map<String, Channel>> entry : channelCache.entrySet()) {
            Class<? extends Channel> channelKlass = entry.getKey();
            Set<String> channelNames = entry.getValue().keySet();
            channelsNotReused.get(channelKlass).addAll(channelNames);
        }

        // 一般情况下 channelNames = [c1]
        Set<String> channelNames = agentConf.getChannelSet();

        // 一般情况下 compMap 等于空
        Map<String, ComponentConfiguration> compMap = agentConf.getChannelConfigMap();
        for (String chName : channelNames) {
            ComponentConfiguration comp = compMap.get(chName);
            if (comp != null) {
                Channel channel = getOrCreateChannel(channelsNotReused, comp.getComponentName(), comp.getType());
                try {
                    Configurables.configure(channel, comp);
                    channelComponentMap.put(comp.getComponentName(),
                            new ChannelComponent(channel));
                    LOGGER.info("Created channel " + chName);
                } catch (Exception e) {
                    String msg = String.format("Channel %s has been removed due to an " +
                            "error during configuration", chName);
                    LOGGER.error(msg, e);
                }
            }
        }

        for (String chName : channelNames) {
            /**
             * 获取 channelName = c1 对应的 Context 配置
             */
            Context context = agentConf.getChannelContext().get(chName);
            if (context != null) {
                /**
                 * 通过反射的方式创建 MemoryChannel 此时去阅读 MemoryChannel 无参构造函数
                 */
                Channel channel = getOrCreateChannel(channelsNotReused, chName, context.getString(BasicConfigurationConstants.CONFIG_TYPE));
                try {
                    /**
                     * 直接调用 MemoryChannel.configure(context) 方法
                     */
                    Configurables.configure(channel, context);

                    /**
                     * 缓存 channel 对应的组件 ChannelComponent(封装了 MemoryChannel)
                     */
                    channelComponentMap.put(chName, new ChannelComponent(channel));
                    LOGGER.info("Created channel " + chName);
                } catch (Exception e) {
                    String msg = String.format("Channel %s has been removed due to an " +
                            "error during configuration", chName);
                    LOGGER.error(msg, e);
                }
            }
        }
        /*
         * Any channel which was not re-used, will have it's reference removed
         */
        // 一般不执行如下代码
        for (Class<? extends Channel> channelKlass : channelsNotReused.keySet()) {
            Map<String, Channel> channelMap = channelCache.get(channelKlass);
            if (channelMap != null) {
                for (String channelName : channelsNotReused.get(channelKlass)) {
                    if (channelMap.remove(channelName) != null) {
                        LOGGER.info("Removed {} of type {}", channelName, channelKlass);
                    }
                }
                if (channelMap.isEmpty()) {
                    channelCache.remove(channelKlass);
                }
            }
        }
    }

    private Channel getOrCreateChannel(
            ListMultimap<Class<? extends Channel>, String> channelsNotReused,
            String name, String type)
            throws FlumeException {

        // MEMORY("org.apache.flume.channel.MemoryChannel")
        Class<? extends Channel> channelClass = channelFactory.getClass(type);
        /*
         * Channel has requested a new instance on each re-configuration
         */
        if (channelClass.isAnnotationPresent(Disposable.class)) {
            Channel channel = channelFactory.create(name, type);
            channel.setName(name);
            return channel;
        }
        Map<String, Channel> channelMap = channelCache.get(channelClass);
        if (channelMap == null) {
            channelMap = new HashMap<String, Channel>();
            channelCache.put(channelClass, channelMap);
        }
        Channel channel = channelMap.get(name);
        if (channel == null) {
            channel = channelFactory.create(name, type);
            channel.setName(name);
            channelMap.put(name, channel);
        }
        channelsNotReused.get(channelClass).remove(name);
        return channel;
    }

    private void loadSources(AgentConfiguration agentConf,
                             Map<String, ChannelComponent> channelComponentMap,
                             Map<String, SourceRunner> sourceRunnerMap)
            throws InstantiationException {

        /**
         * 场景驱动情况下 sourceNames = [r1]
         */
        Set<String> sourceNames = agentConf.getSourceSet();

        /**
         * 一般情况下 compMap 为空
         */
        Map<String, ComponentConfiguration> compMap = agentConf.getSourceConfigMap();
        for (String sourceName : sourceNames) {
            ComponentConfiguration comp = compMap.get(sourceName);
            if (comp != null) {
                SourceConfiguration config = (SourceConfiguration) comp;

                Source source = sourceFactory.create(comp.getComponentName(),
                        comp.getType());
                try {
                    Configurables.configure(source, config);
                    Set<String> channelNames = config.getChannels();
                    List<Channel> sourceChannels =
                            getSourceChannels(channelComponentMap, source, channelNames);
                    if (sourceChannels.isEmpty()) {
                        String msg = String.format("Source %s is not connected to a " +
                                "channel", sourceName);
                        throw new IllegalStateException(msg);
                    }
                    ChannelSelectorConfiguration selectorConfig =
                            config.getSelectorConfiguration();

                    ChannelSelector selector = ChannelSelectorFactory.create(
                            sourceChannels, selectorConfig);

                    ChannelProcessor channelProcessor = new ChannelProcessor(selector);
                    Configurables.configure(channelProcessor, config);

                    source.setChannelProcessor(channelProcessor);
                    sourceRunnerMap.put(comp.getComponentName(),
                            SourceRunner.forSource(source));
                    for (Channel channel : sourceChannels) {
                        ChannelComponent channelComponent =
                                Preconditions.checkNotNull(channelComponentMap.get(channel.getName()),
                                        String.format("Channel %s", channel.getName()));
                        channelComponent.components.add(sourceName);
                    }
                } catch (Exception e) {
                    String msg = String.format("Source %s has been removed due to an " +
                            "error during configuration", sourceName);
                    LOGGER.error(msg, e);
                }
            }
        }

        /**
         * 获取 sources 配置
         */
        Map<String, Context> sourceContexts = agentConf.getSourceContext();
        for (String sourceName : sourceNames) {
            /**
             * 比如 source 配置如下:
             * a1.sources.r1.type = syslogudp
             * a1.sources.r1.port = 5141
             * a1.sources.r1.host = 0.0.0.0
             * a1.sources.r1.channels = c1
             * 解析配置如下:
             * context = { type:syslogudp, port:5141, host:0.0.0.0, channels = c1 }
             */
            Context context = sourceContexts.get(sourceName);
            if (context != null) {
                /**
                 * 基于场景驱动情况下 type = syslogudp 根据 SourceType 源码得出如下枚举值
                 * SYSLOGUDP("org.apache.flume.source.SyslogUDPSource") 故基于反射的方式创建 SyslogUDPSource 对象
                 * 请阅读 SyslogUDPSource 以及父类 AbstractSource 无参构造函数
                 */
                Source source = sourceFactory.create(sourceName, context.getString(BasicConfigurationConstants.CONFIG_TYPE));
                try {
                    /**
                     * 请阅读 SyslogUDPSource.configure(context) 方法
                     */
                    Configurables.configure(source, context);
                    /**
                     * sources 对应的 channels 场景驱动情况下 channelNames = [c1]
                     */
                    String[] channelNames = context.getString(BasicConfigurationConstants.CONFIG_CHANNELS).split("\\s+");
                    /**
                     * 根据 channelNames 获取对应的 Channel 场景驱动情况下 sourceChannels = [MemoryChannel]
                     */
                    List<Channel> sourceChannels = getSourceChannels(channelComponentMap, source, Arrays.asList(channelNames));
                    if (sourceChannels.isEmpty()) {
                        String msg = String.format("Source %s is not connected to a " + "channel", sourceName);
                        throw new IllegalStateException(msg);
                    }

                    /**
                     * 判断 source 配置是否存在 selector 选择器相关配置 场景驱动情况下 selectorConfig 为空
                     */
                    Map<String, String> selectorConfig = context.getSubProperties(BasicConfigurationConstants.CONFIG_SOURCE_CHANNELSELECTOR_PREFIX);

                    /**
                     * 根据 source 对应的 selector. 配置创建对应的 ChannelSelector
                     * 场景驱动情况下 默认创建 ReplicatingChannelSelector 对象并调用其 setChannels(sourceChannels)、configure(context) 方法完成初始化
                     */
                    ChannelSelector selector = ChannelSelectorFactory.create(sourceChannels, selectorConfig);

                    /**
                     * 封装 selector 对象为 ChannelProcessor
                     */
                    ChannelProcessor channelProcessor = new ChannelProcessor(selector);
                    /**
                     * 调用 ChannelProcessor.configure(context) 核心操作初始化拦截器
                     */
                    Configurables.configure(channelProcessor, context);
                    /**
                     * source 连接 channel
                     */
                    source.setChannelProcessor(channelProcessor);

                    /**
                     * 将 source 封装为 SourceRunner 缓存在 sourceRunnerMap
                     * 场景驱动情况下 source = SyslogUDPSource 其实现 EventDrivenSource 接口
                     * r1 - > EventDrivenSource(SyslogUDPSource)
                     */
                    sourceRunnerMap.put(sourceName, SourceRunner.forSource(source));
                    for (Channel channel : sourceChannels) {
                        ChannelComponent channelComponent = Preconditions.checkNotNull(channelComponentMap.get(channel.getName()),
                                String.format("Channel %s", channel.getName()));
                        /**
                         * 具体的 channel 连接具体的 source 名称
                         */
                        channelComponent.components.add(sourceName);
                    }
                } catch (Exception e) {
                    String msg = String.format("Source %s has been removed due to an " +
                            "error during configuration", sourceName);
                    LOGGER.error(msg, e);
                }
            }
        }
    }

    private List<Channel> getSourceChannels(Map<String, ChannelComponent> channelComponentMap,
                                            Source source, Collection<String> channelNames) throws InstantiationException {
        List<Channel> sourceChannels = new ArrayList<Channel>();
        for (String chName : channelNames) {
            ChannelComponent channelComponent = channelComponentMap.get(chName);
            if (channelComponent != null) {
                checkSourceChannelCompatibility(source, channelComponent.channel);
                sourceChannels.add(channelComponent.channel);
            }
        }
        return sourceChannels;
    }

    private void checkSourceChannelCompatibility(Source source, Channel channel)
            throws InstantiationException {
        if (source instanceof BatchSizeSupported && channel instanceof TransactionCapacitySupported) {
            long transCap = ((TransactionCapacitySupported) channel).getTransactionCapacity();
            long batchSize = ((BatchSizeSupported) source).getBatchSize();
            if (transCap < batchSize) {
                String msg = String.format(
                        "Incompatible source and channel settings defined. " +
                                "source's batch size is greater than the channels transaction capacity. " +
                                "Source: %s, batch size = %d, channel %s, transaction capacity = %d",
                        source.getName(), batchSize,
                        channel.getName(), transCap);
                throw new InstantiationException(msg);
            }
        }
    }

    private void checkSinkChannelCompatibility(Sink sink, Channel channel)
            throws InstantiationException {
        if (sink instanceof BatchSizeSupported && channel instanceof TransactionCapacitySupported) {
            long transCap = ((TransactionCapacitySupported) channel).getTransactionCapacity();
            long batchSize = ((BatchSizeSupported) sink).getBatchSize();
            if (transCap < batchSize) {
                String msg = String.format(
                        "Incompatible sink and channel settings defined. " +
                                "sink's batch size is greater than the channels transaction capacity. " +
                                "Sink: %s, batch size = %d, channel %s, transaction capacity = %d",
                        sink.getName(), batchSize,
                        channel.getName(), transCap);
                throw new InstantiationException(msg);
            }
        }
    }

    private void loadSinks(AgentConfiguration agentConf,
                           Map<String, ChannelComponent> channelComponentMap, Map<String, SinkRunner> sinkRunnerMap)
            throws InstantiationException {
        /**
         * 场景驱动情况下 sinkNames = [k1]
         */
        Set<String> sinkNames = agentConf.getSinkSet();
        Map<String, Sink> sinks = new HashMap<String, Sink>();

        /**
         * 一般情况下 compMap 为空
         */
        Map<String, ComponentConfiguration> compMap = agentConf.getSinkConfigMap();
        for (String sinkName : sinkNames) {
            ComponentConfiguration comp = compMap.get(sinkName);
            if (comp != null) {
                SinkConfiguration config = (SinkConfiguration) comp;
                Sink sink = sinkFactory.create(comp.getComponentName(), comp.getType());
                try {
                    Configurables.configure(sink, config);
                    ChannelComponent channelComponent = channelComponentMap.get(config.getChannel());
                    if (channelComponent == null) {
                        String msg = String.format("Sink %s is not connected to a " +
                                "channel", sinkName);
                        throw new IllegalStateException(msg);
                    }
                    checkSinkChannelCompatibility(sink, channelComponent.channel);
                    sink.setChannel(channelComponent.channel);
                    sinks.put(comp.getComponentName(), sink);
                    channelComponent.components.add(sinkName);
                } catch (Exception e) {
                    String msg = String.format("Sink %s has been removed due to an " +
                            "error during configuration", sinkName);
                    LOGGER.error(msg, e);
                }
            }
        }

        /**
         * 假设配置文件 sink 配置如下:
         * a1.sinks.k1.channel = c1
         * a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink
         * a1.sinks.k1.kafka.topic = demo
         * a1.sinks.k1.kafka.bootstrap.servers = 192.168.14.203:9092
         * 解析配置如下:
         * sinkContexts = { channel:c1, type:org.apache.flume.sink.kafka.KafkaSink,  kafka.topic:demo, kafka.bootstrap.servers:192.168.14.203:9092}
         */
        Map<String, Context> sinkContexts = agentConf.getSinkContext();
        for (String sinkName : sinkNames) {
            Context context = sinkContexts.get(sinkName);
            if (context != null) {
                /**
                 * 根据配置 type = xxx 创建具体的 Sink
                 * 场景驱动情况下 创建 KafkaSink
                 */
                Sink sink = sinkFactory.create(sinkName, context.getString(BasicConfigurationConstants.CONFIG_TYPE));
                try {
                    /**
                     * 调用具体 Sink 对象的 configure(context) 执行配置解析获取
                     */
                    Configurables.configure(sink, context);

                    ChannelComponent channelComponent = channelComponentMap.get(context.getString(BasicConfigurationConstants.CONFIG_CHANNEL));
                    if (channelComponent == null) {
                        String msg = String.format("Sink %s is not connected to a " +
                                "channel", sinkName);
                        throw new IllegalStateException(msg);
                    }
                    /**
                     * 检查 Channel 的事务大小大于等于 Sink 的批次大小 否则报异常
                     */
                    checkSinkChannelCompatibility(sink, channelComponent.channel);

                    /**
                     * Sink 连接 Channel
                     * 缓存 Sink
                     * Channel 连接具体的 sinkName
                     */
                    sink.setChannel(channelComponent.channel);
                    sinks.put(sinkName, sink);
                    channelComponent.components.add(sinkName);
                } catch (Exception e) {
                    String msg = String.format("Sink %s has been removed due to an " +
                            "error during configuration", sinkName);
                    LOGGER.error(msg, e);
                }
            }
        }

        /**
         * 创建具体的 Sink 封装提
         */
        loadSinkGroups(agentConf, sinks, sinkRunnerMap);
    }

    private void loadSinkGroups(AgentConfiguration agentConf,
                                Map<String, Sink> sinks, Map<String, SinkRunner> sinkRunnerMap)
            throws InstantiationException {
        /**
         * 暂时不考虑 sinkGroup 情况下
         */
        Set<String> sinkGroupNames = agentConf.getSinkgroupSet();
        Map<String, ComponentConfiguration> compMap =
                agentConf.getSinkGroupConfigMap();
        Map<String, String> usedSinks = new HashMap<String, String>();
        for (String groupName : sinkGroupNames) {
            ComponentConfiguration comp = compMap.get(groupName);
            if (comp != null) {
                SinkGroupConfiguration groupConf = (SinkGroupConfiguration) comp;
                List<Sink> groupSinks = new ArrayList<Sink>();
                for (String sink : groupConf.getSinks()) {
                    Sink s = sinks.remove(sink);
                    if (s == null) {
                        String sinkUser = usedSinks.get(sink);
                        if (sinkUser != null) {
                            throw new InstantiationException(String.format(
                                    "Sink %s of group %s already " +
                                            "in use by group %s", sink, groupName, sinkUser));
                        } else {
                            throw new InstantiationException(String.format(
                                    "Sink %s of group %s does "
                                            + "not exist or is not properly configured", sink,
                                    groupName));
                        }
                    }
                    groupSinks.add(s);
                    usedSinks.put(sink, groupName);
                }
                try {
                    SinkGroup group = new SinkGroup(groupSinks);
                    Configurables.configure(group, groupConf);
                    sinkRunnerMap.put(comp.getComponentName(),
                            new SinkRunner(group.getProcessor()));
                } catch (Exception e) {
                    String msg = String.format("SinkGroup %s has been removed due to " +
                            "an error during configuration", groupName);
                    LOGGER.error(msg, e);
                }
            }
        }

        /**
         * 将 Sink 对象封装为 SinkRunner
         */
        for (Entry<String, Sink> entry : sinks.entrySet()) {
            if (!usedSinks.containsValue(entry.getKey())) {
                try {
                    SinkProcessor pr = new DefaultSinkProcessor();
                    List<Sink> sinkMap = new ArrayList<Sink>();
                    sinkMap.add(entry.getValue());
                    pr.setSinks(sinkMap);
                    Configurables.configure(pr, new Context());
                    /**
                     * k1 -> SinkRunner(DefaultSinkProcessor(KafkaSink))
                     */
                    sinkRunnerMap.put(entry.getKey(), new SinkRunner(pr));
                } catch (Exception e) {
                    String msg = String.format("SinkGroup %s has been removed due to " +
                            "an error during configuration", entry.getKey());
                    LOGGER.error(msg, e);
                }
            }
        }
    }

    private static class ChannelComponent {
        final Channel channel;
        final List<String> components;

        ChannelComponent(Channel channel) {
            this.channel = channel;
            components = Lists.newArrayList();
        }
    }

    protected Map<String, String> toMap(Properties properties) {
        Map<String, String> result = Maps.newHashMap();
        Enumeration<?> propertyNames = properties.propertyNames();
        while (propertyNames.hasMoreElements()) {
            String name = (String) propertyNames.nextElement();
            String value = properties.getProperty(name);
            result.put(name, value);
        }
        return result;
    }
}