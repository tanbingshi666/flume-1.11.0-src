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

package org.apache.flume;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * A driver for {@linkplain Sink sinks} that polls them, attempting to
 * {@linkplain Sink#process() process} events if any are available in the
 * {@link Channel}.
 * </p>
 *
 * <p>
 * Note that, unlike {@linkplain Source sources}, all sinks are polled.
 * </p>
 *
 * @see org.apache.flume.Sink
 * @see org.apache.flume.SourceRunner
 */
public class SinkRunner implements LifecycleAware {

    private static final Logger logger = LoggerFactory
            .getLogger(SinkRunner.class);
    private static final long backoffSleepIncrement = 1000;
    private static final long maxBackoffSleep = 5000;

    private CounterGroup counterGroup;
    private PollingRunner runner;
    private Thread runnerThread;
    private LifecycleState lifecycleState;

    private SinkProcessor policy;

    public SinkRunner() {
        counterGroup = new CounterGroup();
        lifecycleState = LifecycleState.IDLE;
    }

    public SinkRunner(SinkProcessor policy) {
        this();
        setSink(policy);
    }

    public SinkProcessor getPolicy() {
        return policy;
    }

    public void setSink(SinkProcessor policy) {
        this.policy = policy;
    }

    @Override
    public void start() {
        /**
         * 场景驱动下 policy = DefaultSinkProcessor
         * 调用 DefaultSinkProcessor 父类 AbstractSingleSinkProcessor.start()
         * 最终调用 KafkaSink.start() 初始化 KafkaProducer 对象
         */
        SinkProcessor policy = getPolicy();
        policy.start();

        /**
         * 创建 sink 从 channel 拉取数据线程 (实现 Runnable 接口)
         */
        runner = new PollingRunner();

        runner.policy = policy;
        runner.counterGroup = counterGroup;
        runner.shouldStop = new AtomicBoolean();

        /**
         * 创建 Thread 线程封装 PollingRunner 并启动
         * 其中 PollingRunner.run() 会一直运行之后接收到终止信息
         */
        runnerThread = new Thread(runner);
        runnerThread.setName("SinkRunner-PollingRunner-" + policy.getClass().getSimpleName());
        runnerThread.start();

        lifecycleState = LifecycleState.START;
    }

    @Override
    public void stop() {

        if (runnerThread != null) {
            runner.shouldStop.set(true);
            runnerThread.interrupt();

            while (runnerThread.isAlive()) {
                try {
                    logger.debug("Waiting for runner thread to exit");
                    runnerThread.join(500);
                } catch (InterruptedException e) {
                    logger.debug("Interrupted while waiting for runner thread to exit. Exception follows.",
                            e);
                }
            }
        }

        getPolicy().stop();
        lifecycleState = LifecycleState.STOP;
    }

    @Override
    public String toString() {
        return "SinkRunner: { policy:" + getPolicy() + " counterGroup:"
                + counterGroup + " }";
    }

    @Override
    public LifecycleState getLifecycleState() {
        return lifecycleState;
    }

    /**
     * {@link Runnable} that {@linkplain SinkProcessor#process() polls} a
     * {@link SinkProcessor} and manages event delivery notification,
     * {@link Sink.Status BACKOFF} delay handling, etc.
     */
    public static class PollingRunner implements Runnable {

        /**
         * 场景驱动情况下 policy = DefaultSinkProcessor
         */
        private SinkProcessor policy;
        private AtomicBoolean shouldStop;
        private CounterGroup counterGroup;

        @Override
        public void run() {
            logger.debug("Polling sink runner starting");

            while (!shouldStop.get()) {
                try {
                    /**
                     * sink 拉取 channel 的数据
                     * 场景驱动情况下 最终调用 KafkaSink.process()
                     * 每一批次[默认100条数据]执行一个事务 完成一个 process() 方法
                     */
                    if (policy.process().equals(Sink.Status.BACKOFF)) {
                        counterGroup.incrementAndGet("runner.backoffs");

                        Thread.sleep(Math.min(counterGroup.incrementAndGet("runner.backoffs.consecutive") * backoffSleepIncrement, maxBackoffSleep));
                    } else {
                        counterGroup.set("runner.backoffs.consecutive", 0L);
                    }
                } catch (InterruptedException e) {
                    logger.debug("Interrupted while processing an event. Exiting.");
                    counterGroup.incrementAndGet("runner.interruptions");
                } catch (Exception e) {
                    logger.error("Unable to deliver event. Exception follows.", e);
                    if (e instanceof EventDeliveryException) {
                        counterGroup.incrementAndGet("runner.deliveryErrors");
                    } else {
                        counterGroup.incrementAndGet("runner.errors");
                    }
                    try {
                        Thread.sleep(maxBackoffSleep);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
            logger.debug("Polling runner exiting. Metrics:{}", counterGroup);
        }

    }
}
