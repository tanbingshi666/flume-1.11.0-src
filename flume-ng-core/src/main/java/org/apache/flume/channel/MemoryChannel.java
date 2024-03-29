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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.flume.ChannelException;
import org.apache.flume.ChannelFullException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.annotations.InterfaceAudience;
import org.apache.flume.annotations.InterfaceStability;
import org.apache.flume.annotations.Recyclable;
import org.apache.flume.conf.TransactionCapacitySupported;
import org.apache.flume.instrumentation.ChannelCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * MemoryChannel is the recommended channel to use when speeds which
 * writing to disk is impractical is required or durability of data is not
 * required.
 * </p>
 * <p>
 * Additionally, MemoryChannel should be used when a channel is required for
 * unit testing purposes.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
@Recyclable
public class MemoryChannel extends BasicChannelSemantics implements TransactionCapacitySupported {
    private static Logger LOGGER = LoggerFactory.getLogger(MemoryChannel.class);
    private static final Integer defaultCapacity = 100;
    private static final Integer defaultTransCapacity = 100;
    private static final double byteCapacitySlotSize = 100;
    private static final Long defaultByteCapacity = (long) (Runtime.getRuntime().maxMemory() * .80);
    private static final Integer defaultByteCapacityBufferPercentage = 20;

    private static final Integer defaultKeepAlive = 3;

    private class MemoryTransaction extends BasicTransactionSemantics {
        private LinkedBlockingDeque<Event> takeList;
        private LinkedBlockingDeque<Event> putList;
        private final ChannelCounter channelCounter;
        private int putByteCounter = 0;
        private int takeByteCounter = 0;

        public MemoryTransaction(int transCapacity, ChannelCounter counter) {
            /**
             * 默认情况下 channel 的事务大小为 100 (transactionCapacity = 100)
             * 初始化两个阻塞队列 (线程安全) 初始化大小都等于 channel 事务大小
             * putList 缓存 source 推送的 Event
             * takeList 缓存 sink 拉取的 Event
             */
            putList = new LinkedBlockingDeque<Event>(transCapacity);
            takeList = new LinkedBlockingDeque<Event>(transCapacity);

            /**
             * channel 的监控指标对象
             */
            channelCounter = counter;
        }

        @Override
        protected void doPut(Event event) throws InterruptedException {
            /**
             * channel 监控指标自增 1 表示 channel put event 尝试次数
             * channel.event.put.attempt -> 1++
             */
            channelCounter.incrementEventPutAttemptCount();

            /**
             * 将 Event 的 body 字节大小除于 100 结果向上取整
             */
            int eventByteSize = (int) Math.ceil(estimateEventSize(event) / byteCapacitySlotSize);

            /**
             * 将 Event 缓存在 putList 阻塞队列中
             * 默认情况下追加到 putList 的末尾 如果不能 put Event 说明该 putList 阻塞队列 (默认容量为 100) 满了
             */
            if (!putList.offer(event)) {
                throw new ChannelException(
                        "Put queue for MemoryTransaction of capacity " +
                                putList.size() + " full, consider committing more frequently, " +
                                "increasing capacity or increasing thread count");
            }

            /**
             * 字节累加
             */
            putByteCounter += eventByteSize;
        }

        @Override
        protected Event doTake() throws InterruptedException {
            /**
             * channel 监控指标累加
             * channel.event.take.attempt  -> 1++
             */
            channelCounter.incrementEventTakeAttemptCount();

            if (takeList.remainingCapacity() == 0) {
                throw new ChannelException("Take list for MemoryTransaction, capacity " +
                        takeList.size() + " full, consider committing more frequently, " +
                        "increasing capacity, or increasing thread count");
            }
            if (!queueStored.tryAcquire(keepAlive, TimeUnit.SECONDS)) {
                return null;
            }

            Event event;
            synchronized (queueLock) {
                /**
                 * 从阻塞队列 queue 的头部获取 event 如果没有则返回 null
                 */
                event = queue.poll();
            }
            Preconditions.checkNotNull(event, "Queue.poll returned NULL despite semaphore " + "signalling existence of entry");
            /**
             * 将从阻塞队列中获取 event 引用条件到 take 的阻塞队列
             */
            takeList.put(event);

            /**
             * 计算 event 字节大小 用于 take、put 阻塞队列的信号量
             */
            int eventByteSize = (int) Math.ceil(estimateEventSize(event) / byteCapacitySlotSize);
            takeByteCounter += eventByteSize;

            return event;
        }

        @Override
        protected void doCommit() throws InterruptedException {
            /**
             * sink 消费一般需要大于 source 产生 event 速度 否则需要等待
             */
            int remainingChange = takeList.size() - putList.size();
            if (remainingChange < 0) {
                if (!bytesRemaining.tryAcquire(putByteCounter, keepAlive, TimeUnit.SECONDS)) {
                    throw new ChannelException("Cannot commit transaction. Byte capacity " +
                            "allocated to store event body " + byteCapacity * byteCapacitySlotSize +
                            "reached. Please increase heap space/byte capacity allocated to " +
                            "the channel as the sinks may not be keeping up with the sources");
                }
                if (!queueRemaining.tryAcquire(-remainingChange, keepAlive, TimeUnit.SECONDS)) {
                    bytesRemaining.release(putByteCounter);
                    throw new ChannelFullException("Space for commit to queue couldn't be acquired." +
                            " Sinks are likely not keeping up with sources, or the buffer size is too tight");
                }
            }
            int puts = putList.size();
            int takes = takeList.size();

            synchronized (queueLock) {
                /**
                 * 将 putList 的 Events 移动到 queue (阻塞队列 默认大小为 100) 队列中
                 */
                if (puts > 0) {
                    while (!putList.isEmpty()) {
                        if (!queue.offer(putList.removeFirst())) {
                            throw new RuntimeException("Queue add failed, this shouldn't be able to happen");
                        }
                    }
                }
                putList.clear();
                takeList.clear();
            }

            /**
             * 不考虑 load balance 情况下
             */
            bytesRemaining.release(takeByteCounter);
            takeByteCounter = 0;
            putByteCounter = 0;
            queueStored.release(puts);
            if (remainingChange > 0) {
                queueRemaining.release(remainingChange);
            }

            /**
             * channel 监控指标 channel.event.put.success -> + puts
             */
            if (puts > 0) {
                channelCounter.addToEventPutSuccessCount(puts);
            }
            /**
             * channel 监控指标 channel.event.take.success -> + takes
             */
            if (takes > 0) {
                channelCounter.addToEventTakeSuccessCount(takes);
            }

            /**
             * channel 监控指标 channel.current.size -> 当前 queue 大小
             */
            channelCounter.setChannelSize(queue.size());
        }

        @Override
        protected void doRollback() {
            int takes = takeList.size();
            synchronized (queueLock) {
                Preconditions.checkState(queue.remainingCapacity() >= takeList.size(),
                        "Not enough space in memory channel " +
                                "queue to rollback takes. This should never happen, please report");
                while (!takeList.isEmpty()) {
                    queue.addFirst(takeList.removeLast());
                }
                putList.clear();
            }
            putByteCounter = 0;
            takeByteCounter = 0;

            queueStored.release(takes);
            channelCounter.setChannelSize(queue.size());
        }

    }

    // lock to guard queue, mainly needed to keep it locked down during resizes
    // it should never be held through a blocking operation
    private Object queueLock = new Object();

    @GuardedBy(value = "queueLock")
    private LinkedBlockingDeque<Event> queue;

    // invariant that tracks the amount of space remaining in the queue(with all uncommitted takeLists deducted)
    // we maintain the remaining permits = queue.remaining - takeList.size()
    // this allows local threads waiting for space in the queue to commit without denying access to the
    // shared lock to threads that would make more space on the queue
    private Semaphore queueRemaining;

    // used to make "reservations" to grab data from the queue.
    // by using this we can block for a while to get data without locking all other threads out
    // like we would if we tried to use a blocking call on queue
    private Semaphore queueStored;

    // maximum items in a transaction queue
    private volatile Integer transCapacity;
    private volatile int keepAlive;
    private volatile int byteCapacity;
    private volatile int lastByteCapacity;
    private volatile int byteCapacityBufferPercentage;
    private Semaphore bytesRemaining;
    private ChannelCounter channelCounter;

    public MemoryChannel() {
        super();
    }

    /**
     * Read parameters from context
     * <li>capacity = type long that defines the total number of events allowed at one time in the queue.
     * <li>transactionCapacity = type long that defines the total number of events allowed in one transaction.
     * <li>byteCapacity = type long that defines the max number of bytes used for events in the queue.
     * <li>byteCapacityBufferPercentage = type int that defines the percent of buffer between byteCapacity and the estimated event size.
     * <li>keep-alive = type int that defines the number of second to wait for a queue permit
     */
    @Override
    public void configure(Context context) {
        /**
         * Context 本质是一个 HashMap 缓存了 MemoryChannel 的配置信息
         */
        Integer capacity = null;
        try {
            capacity = context.getInteger("capacity", defaultCapacity);
        } catch (NumberFormatException e) {
            capacity = defaultCapacity;
            LOGGER.warn("Invalid capacity specified, initializing channel to "
                    + "default capacity of {}", defaultCapacity);
        }

        if (capacity <= 0) {
            capacity = defaultCapacity;
            LOGGER.warn("Invalid capacity specified, initializing channel to "
                    + "default capacity of {}", defaultCapacity);
        }
        try {
            transCapacity = context.getInteger("transactionCapacity", defaultTransCapacity);
        } catch (NumberFormatException e) {
            transCapacity = defaultTransCapacity;
            LOGGER.warn("Invalid transation capacity specified, initializing channel"
                    + " to default capacity of {}", defaultTransCapacity);
        }

        if (transCapacity <= 0) {
            transCapacity = defaultTransCapacity;
            LOGGER.warn("Invalid transation capacity specified, initializing channel"
                    + " to default capacity of {}", defaultTransCapacity);
        }
        Preconditions.checkState(transCapacity <= capacity,
                "Transaction Capacity of Memory Channel cannot be higher than " +
                        "the capacity.");

        try {
            byteCapacityBufferPercentage = context.getInteger("byteCapacityBufferPercentage", defaultByteCapacityBufferPercentage);
        } catch (NumberFormatException e) {
            byteCapacityBufferPercentage = defaultByteCapacityBufferPercentage;
        }

        try {
            byteCapacity = (int) ((context.getLong("byteCapacity", defaultByteCapacity).longValue() *
                    (1 - byteCapacityBufferPercentage * .01)) / byteCapacitySlotSize);
            if (byteCapacity < 1) {
                byteCapacity = Integer.MAX_VALUE;
            }
        } catch (NumberFormatException e) {
            byteCapacity = (int) ((defaultByteCapacity * (1 - byteCapacityBufferPercentage * .01)) /
                    byteCapacitySlotSize);
        }

        try {
            keepAlive = context.getInteger("keep-alive", defaultKeepAlive);
        } catch (NumberFormatException e) {
            keepAlive = defaultKeepAlive;
        }

        if (queue != null) {
            try {
                resizeQueue(capacity);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        } else {
            synchronized (queueLock) {
                // 创建阻塞队列 默认大小为 100
                queue = new LinkedBlockingDeque<Event>(capacity);
                queueRemaining = new Semaphore(capacity);
                queueStored = new Semaphore(0);
            }
        }

        if (bytesRemaining == null) {
            // 创建 Semaphore
            bytesRemaining = new Semaphore(byteCapacity);
            lastByteCapacity = byteCapacity;
        } else {
            if (byteCapacity > lastByteCapacity) {
                bytesRemaining.release(byteCapacity - lastByteCapacity);
                lastByteCapacity = byteCapacity;
            } else {
                try {
                    if (!bytesRemaining.tryAcquire(lastByteCapacity - byteCapacity, keepAlive,
                            TimeUnit.SECONDS)) {
                        LOGGER.warn("Couldn't acquire permits to downsize the byte capacity, resizing has been aborted");
                    } else {
                        lastByteCapacity = byteCapacity;
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        if (channelCounter == null) {
            channelCounter = new ChannelCounter(getName());
        }
    }

    private void resizeQueue(int capacity) throws InterruptedException {
        int oldCapacity;
        synchronized (queueLock) {
            oldCapacity = queue.size() + queue.remainingCapacity();
        }

        if (oldCapacity == capacity) {
            return;
        } else if (oldCapacity > capacity) {
            if (!queueRemaining.tryAcquire(oldCapacity - capacity, keepAlive, TimeUnit.SECONDS)) {
                LOGGER.warn("Couldn't acquire permits to downsize the queue, resizing has been aborted");
            } else {
                synchronized (queueLock) {
                    LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(capacity);
                    newQueue.addAll(queue);
                    queue = newQueue;
                }
            }
        } else {
            synchronized (queueLock) {
                LinkedBlockingDeque<Event> newQueue = new LinkedBlockingDeque<Event>(capacity);
                newQueue.addAll(queue);
                queue = newQueue;
            }
            queueRemaining.release(capacity - oldCapacity);
        }
    }

    @Override
    public synchronized void start() {
        /**
         * 初始化监控 Counter 并启动
         */
        channelCounter.start();
        /**
         * 往监控指标的 Counter 添加 channel.current.size = 100 指标
         */
        channelCounter.setChannelSize(queue.size());
        /**
         * 往监控指标的 Counter 添加 channel.capacity = 100
         */
        channelCounter.setChannelCapacity(Long.valueOf(queue.size() + queue.remainingCapacity()));
        /**
         * 表示 Channel 状态为 START
         */
        super.start();
    }

    @Override
    public synchronized void stop() {
        channelCounter.setChannelSize(queue.size());
        channelCounter.stop();
        super.stop();
    }

    @Override
    protected BasicTransactionSemantics createTransaction() {
        /**
         * MemoryChannel 对象 MemoryTransaction 事务
         */
        return new MemoryTransaction(transCapacity, channelCounter);
    }

    private long estimateEventSize(Event event) {
        byte[] body = event.getBody();
        if (body != null && body.length != 0) {
            return body.length;
        }
        //Each event occupies at least 1 slot, so return 1.
        return 1;
    }

    @VisibleForTesting
    int getBytesRemainingValue() {
        return bytesRemaining.availablePermits();
    }

    public long getTransactionCapacity() {
        return transCapacity;
    }
}
