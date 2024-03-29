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
package org.apache.flume.source;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.instrumentation.SourceCounter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.DatagramPacket;
import io.netty.channel.socket.nio.NioDatagramChannel;

public class SyslogUDPSource extends AbstractSource implements EventDrivenSource, Configurable {

    // Default Min size
    public static final int DEFAULT_MIN_SIZE = 2048;
    public static final int DEFAULT_INITIAL_SIZE = DEFAULT_MIN_SIZE;
    private static final Logger logger = LoggerFactory.getLogger(SyslogUDPSource.class);
    private final int maxsize = 1 << 16; // 64k is max allowable in RFC 5426
    private int port;
    private String host = null;
    private Map<String, String> formaterProp;
    private Set<String> keepFields;
    private String clientIPHeader;
    private String clientHostnameHeader;
    private EventLoopGroup group;
    private Channel channel;
    private SourceCounter sourceCounter;

    @Override
    public void start() {
        // setup Netty server
        /**
         * 基于 Netty 启动 UDP
         */
        group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioDatagramChannel.class)
                    .option(ChannelOption.SO_BROADCAST, true)
                    /**
                     * 数据接收和处理 Handler
                     */
                    .handler(new SyslogUdpHandler(formaterProp, keepFields, clientIPHeader, clientHostnameHeader));
            if (host == null) {
                channel = b.bind(port).sync().channel();
            } else {
                channel = b.bind(host, port).sync().channel();
            }
        } catch (InterruptedException ex) {
            logger.warn("netty server startup was interrupted", ex);
        }

        /**
         * 初始化和启动 Counter
         */
        sourceCounter.start();
        /**
         * 状态修改为 START
         */
        super.start();
    }

    @Override
    public void stop() {
        logger.info("Syslog UDP Source stopping...");
        logger.info("Metrics: {}", sourceCounter);
        group.shutdownGracefully();
        sourceCounter.stop();
        super.stop();
    }

    @Override
    public void configure(Context context) {
        /**
         * 比如 source 配置如下:
         * a1.sources.r1.type = syslogudp
         * a1.sources.r1.port = 5141
         * a1.sources.r1.host = 0.0.0.0
         * a1.sources.r1.channels = c1
         * 解析配置如下:
         * context = { type:syslogudp, port:5141, host:0.0.0.0, channels = c1 }
         */
        Configurables.ensureRequiredNonNull(context, SyslogSourceConfigurationConstants.CONFIG_PORT);
        port = context.getInteger(SyslogSourceConfigurationConstants.CONFIG_PORT);
        host = context.getString(SyslogSourceConfigurationConstants.CONFIG_HOST);
        formaterProp = context.getSubProperties(SyslogSourceConfigurationConstants.CONFIG_FORMAT_PREFIX);
        keepFields = SyslogUtils.chooseFieldsToKeep(
                context.getString(
                        SyslogSourceConfigurationConstants.CONFIG_KEEP_FIELDS,
                        SyslogSourceConfigurationConstants.DEFAULT_KEEP_FIELDS));
        clientIPHeader = context.getString(SyslogSourceConfigurationConstants.CONFIG_CLIENT_IP_HEADER);
        clientHostnameHeader = context.getString(SyslogSourceConfigurationConstants.CONFIG_CLIENT_HOSTNAME_HEADER);

        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
    }

    @VisibleForTesting
    InetSocketAddress getBoundAddress() {
        SocketAddress localAddress = channel.localAddress();
        if (!(localAddress instanceof InetSocketAddress)) {
            throw new IllegalArgumentException("Not bound to an internet address");
        }
        return (InetSocketAddress) localAddress;
    }

    @VisibleForTesting
    SourceCounter getSourceCounter() {
        return sourceCounter;
    }

    public class SyslogUdpHandler extends SimpleChannelInboundHandler<DatagramPacket> {
        SyslogUtils syslogUtils = new SyslogUtils(DEFAULT_INITIAL_SIZE, null, true);
        private final String clientIPHeader;
        private final String clientHostnameHeader;

        SyslogUdpHandler(Map<String, String> formatProps, Set<String> keepFields, String clientIPHeader,
                         String clientHostnameHeader) {
            syslogUtils.addFormats(formatProps);
            syslogUtils.setKeepFields(keepFields);
            this.clientIPHeader = clientIPHeader;
            this.clientHostnameHeader = clientHostnameHeader;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, DatagramPacket packet) throws Exception {
            try {
                /**
                 * UDP 服务接收一条数据最大大小为 64kb 最小为 2kb
                 */
                syslogUtils.setEventSize(maxsize);

                /**
                 * 接收数据解析为 Flume Event
                 */
                Event e = syslogUtils.extractEvent(packet.content());
                if (e == null) {
                    return;
                }

                /**
                 * 如果在配置文件中指定 a1.sources.r1.clientIPHeader = report_ip
                 * 则 clientIPHeader = report_ip 故将客户端的 IP 设置在 Event 的头部 (比如: report_ip->192.169.14.203)
                 */
                if (clientIPHeader != null) {
                    e.getHeaders().put(clientIPHeader, SyslogUtils.getIP(packet.sender()));
                }

                /**
                 * 与 clientIPHeader 相同的逻辑
                 * a1.sources.r1.clientHostnameHeader = report_hostname  -> Event Header (report_hostname->bigdata-3)
                 */
                if (clientHostnameHeader != null) {
                    e.getHeaders().put(clientHostnameHeader, SyslogUtils.getHostname(packet.sender()));
                }

                /**
                 * source counter 自增 1 表示 received 到一条记录
                 * src.events.received -> 1++
                 */
                sourceCounter.incrementEventReceivedCount();

                /**
                 * 将 Event 推送到 Channel (如果配置了 拦截器则先执行在推送)
                 */
                getChannelProcessor().processEvent(e);

                /**
                 * source counter 自增 1 表示 accepted 到一条记录
                 * src.events.accepted -> 1++
                 */
                sourceCounter.incrementEventAcceptedCount();
            } catch (ChannelException ex) {
                logger.error("Error writting to channel", ex);
                sourceCounter.incrementChannelWriteFail();
            } catch (RuntimeException ex) {
                logger.error("Error parsing event from syslog stream, event dropped", ex);
                sourceCounter.incrementEventReadFail();
            }
        }
    }
}
