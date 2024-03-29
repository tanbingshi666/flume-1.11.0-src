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
package org.apache.flume.channel;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.FlumeException;
import org.apache.flume.interceptor.Interceptor;
import org.apache.flume.interceptor.InterceptorChain;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.interceptor.InterceptorBuilderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A channel processor exposes operations to put {@link Event}s into
 * {@link Channel}s. These operations will propagate a {@link ChannelException}
 * if any errors occur while attempting to write to {@code required} channels.
 * <p>
 * Each channel processor instance is configured with a {@link ChannelSelector}
 * instance that specifies which channels are
 * {@linkplain ChannelSelector#getRequiredChannels(Event) required} and which
 * channels are
 * {@linkplain ChannelSelector#getOptionalChannels(Event) optional}.
 */
public class ChannelProcessor implements Configurable {

    private static final Logger LOG = LoggerFactory.getLogger(
            ChannelProcessor.class);

    private final ChannelSelector selector;
    private final InterceptorChain interceptorChain;

    public ChannelProcessor(ChannelSelector selector) {
        /**
         * 场景驱动下 selector = ReplicatingChannelSelector
         */
        this.selector = selector;
        /**
         * 拦截器链对象创建
         */
        this.interceptorChain = new InterceptorChain();
    }

    public void initialize() {
        interceptorChain.initialize();
    }

    public void close() {
        interceptorChain.close();
    }

    /**
     * The Context of the associated Source is passed.
     *
     * @param context
     */
    @Override
    public void configure(Context context) {
        /**
         * 初始化拦截器
         */
        configureInterceptors(context);
    }

    // WARNING: throws FlumeException (is that ok?)
    private void configureInterceptors(Context context) {
        /**
         * 假设配置文件 source 内容为如下:
         * a1.sources.r1.type = syslogudp
         * a1.sources.r1.port = 5141
         * a1.sources.r1.host = 0.0.0.0
         * a1.sources.r1.channels = c1
         *
         * a1.sources.r1.interceptors = i1
         * a1.sources.r1.interceptors.i1.type = org.apache.flume.interceptor.TimestampInterceptor$Builder
         *
         * context = {type:syslogudp, port:5141, host:0.0.0.0, channels:c1, interceptors:i1, interceptors.i1.type:org.apache.flume.interceptor.TimestampInterceptor$Builder}
         */

        List<Interceptor> interceptors = Lists.newLinkedList();

        /**
         * 获取拦截器字符串
         * 暂时不考虑拦截器
         */
        String interceptorListStr = context.getString("interceptors", "");
        if (interceptorListStr.isEmpty()) {
            return;
        }

        String[] interceptorNames = interceptorListStr.split("\\s+");

        Context interceptorContexts =
                new Context(context.getSubProperties("interceptors."));

        // run through and instantiate all the interceptors specified in the Context
        InterceptorBuilderFactory factory = new InterceptorBuilderFactory();
        for (String interceptorName : interceptorNames) {
            Context interceptorContext = new Context(
                    interceptorContexts.getSubProperties(interceptorName + "."));
            String type = interceptorContext.getString("type");
            if (type == null) {
                LOG.error("Type not specified for interceptor " + interceptorName);
                throw new FlumeException("Interceptor.Type not specified for " +
                        interceptorName);
            }
            try {
                Interceptor.Builder builder = factory.newInstance(type);
                builder.configure(interceptorContext);
                interceptors.add(builder.build());
            } catch (ClassNotFoundException e) {
                LOG.error("Builder class not found. Exception follows.", e);
                throw new FlumeException("Interceptor.Builder not found.", e);
            } catch (InstantiationException e) {
                LOG.error("Could not instantiate Builder. Exception follows.", e);
                throw new FlumeException("Interceptor.Builder not constructable.", e);
            } catch (IllegalAccessException e) {
                LOG.error("Unable to access Builder. Exception follows.", e);
                throw new FlumeException("Unable to access Interceptor.Builder.", e);
            }
        }

        interceptorChain.setInterceptors(interceptors);
    }

    public ChannelSelector getSelector() {
        return selector;
    }

    /**
     * Attempts to {@linkplain Channel#put(Event) put} the given events into each
     * configured channel. If any {@code required} channel throws a
     * {@link ChannelException}, that exception will be propagated.
     * <p>
     * <p>Note that if multiple channels are configured, some {@link Transaction}s
     * may have already been committed while others may be rolled back in the
     * case of an exception.
     *
     * @param events A list of events to put into the configured channels.
     * @throws ChannelException when a write to a required channel fails.
     */
    public void processEventBatch(List<Event> events) {
        Preconditions.checkNotNull(events, "Event list must not be null");

        events = interceptorChain.intercept(events);

        Map<Channel, List<Event>> reqChannelQueue =
                new LinkedHashMap<Channel, List<Event>>();

        Map<Channel, List<Event>> optChannelQueue =
                new LinkedHashMap<Channel, List<Event>>();

        for (Event event : events) {
            List<Channel> reqChannels = selector.getRequiredChannels(event);

            for (Channel ch : reqChannels) {
                List<Event> eventQueue = reqChannelQueue.get(ch);
                if (eventQueue == null) {
                    eventQueue = new ArrayList<Event>();
                    reqChannelQueue.put(ch, eventQueue);
                }
                eventQueue.add(event);
            }

            List<Channel> optChannels = selector.getOptionalChannels(event);

            for (Channel ch : optChannels) {
                List<Event> eventQueue = optChannelQueue.get(ch);
                if (eventQueue == null) {
                    eventQueue = new ArrayList<Event>();
                    optChannelQueue.put(ch, eventQueue);
                }

                eventQueue.add(event);
            }
        }

        // Process required channels
        for (Channel reqChannel : reqChannelQueue.keySet()) {
            Transaction tx = reqChannel.getTransaction();
            Preconditions.checkNotNull(tx, "Transaction object must not be null");
            try {
                tx.begin();

                List<Event> batch = reqChannelQueue.get(reqChannel);

                for (Event event : batch) {
                    reqChannel.put(event);
                }

                tx.commit();
            } catch (Throwable t) {
                tx.rollback();
                if (t instanceof Error) {
                    LOG.error("Error while writing to required channel: " + reqChannel, t);
                    throw (Error) t;
                } else if (t instanceof ChannelException) {
                    throw (ChannelException) t;
                } else {
                    throw new ChannelException("Unable to put batch on required " +
                            "channel: " + reqChannel, t);
                }
            } finally {
                if (tx != null) {
                    tx.close();
                }
            }
        }

        // Process optional channels
        for (Channel optChannel : optChannelQueue.keySet()) {
            Transaction tx = optChannel.getTransaction();
            Preconditions.checkNotNull(tx, "Transaction object must not be null");
            try {
                tx.begin();

                List<Event> batch = optChannelQueue.get(optChannel);

                for (Event event : batch) {
                    optChannel.put(event);
                }

                tx.commit();
            } catch (Throwable t) {
                tx.rollback();
                LOG.error("Unable to put batch on optional channel: " + optChannel, t);
                if (t instanceof Error) {
                    throw (Error) t;
                }
            } finally {
                if (tx != null) {
                    tx.close();
                }
            }
        }
    }

    /**
     * Attempts to {@linkplain Channel#put(Event) put} the given event into each
     * configured channel. If any {@code required} channel throws a
     * {@link ChannelException}, that exception will be propagated.
     * <p>
     * <p>Note that if multiple channels are configured, some {@link Transaction}s
     * may have already been committed while others may be rolled back in the
     * case of an exception.
     *
     * @param event The event to put into the configured channels.
     * @throws ChannelException when a write to a required channel fails.
     */
    public void processEvent(Event event) {

        /**
         * 将 Event 经过拦截器处理
         * 暂时不考虑拦截器
         */
        event = interceptorChain.intercept(event);
        if (event == null) {
            return;
        }

        // Process required channels
        /**
         * ChannelSelector 有三种
         * 1. MultiplexingChannelSelector: 需要根据 Event 的头部相关信息选择对应的 Channel
         * 2. ReplicatingChannelSelector: 将 Event 发送全部 Channel
         * 3. LoadBalancingChannelSelector: 负载均衡方式
         * 场景驱动情况下 也即 ReplicatingChannelSelector 也是默认的 Channel Selector 返回一个 MemoryChannel
         */
        List<Channel> requiredChannels = selector.getRequiredChannels(event);
        for (Channel reqChannel : requiredChannels) {
            /**
             * 场景驱动情况下 reqChannel = MemoryChannel
             * 获取 put 事务 (没有则创建)
             */
            Transaction tx = reqChannel.getTransaction();
            Preconditions.checkNotNull(tx, "Transaction object must not be null");
            try {
                /**
                 * 开始 put 事务
                 * 场景驱动情况下 暂时不做啥
                 */
                tx.begin();

                /**
                 * 将 Event 推送到事务的阻塞队列中等待 sink 拉取
                 */
                reqChannel.put(event);

                /**
                 * 提交 put 事务
                 */
                tx.commit();
            } catch (Throwable t) {
                tx.rollback();
                if (t instanceof Error) {
                    LOG.error("Error while writing to required channel: " + reqChannel, t);
                    throw (Error) t;
                } else if (t instanceof ChannelException) {
                    throw (ChannelException) t;
                } else {
                    throw new ChannelException("Unable to put event on required " +
                            "channel: " + reqChannel, t);
                }
            } finally {
                /**
                 * 关闭事务
                 */
                if (tx != null) {
                    tx.close();
                }
            }
        }

        // Process optional channels
        /**
         * 判断是否有可选 channel 如果有 则执行即可
         * 场景驱动情况下 optionalChannels 为空 故不需要往下看了
         */
        List<Channel> optionalChannels = selector.getOptionalChannels(event);
        for (Channel optChannel : optionalChannels) {
            Transaction tx = null;
            try {
                tx = optChannel.getTransaction();
                tx.begin();

                optChannel.put(event);

                tx.commit();
            } catch (Throwable t) {
                tx.rollback();
                LOG.error("Unable to put event on optional channel: " + optChannel, t);
                if (t instanceof Error) {
                    throw (Error) t;
                }
            } finally {
                if (tx != null) {
                    tx.close();
                }
            }
        }
    }
}
