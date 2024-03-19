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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Sink;
import org.apache.flume.SinkProcessor;
import org.apache.flume.SinkRunner;
import org.apache.flume.Source;
import org.apache.flume.SourceRunner;
import org.apache.flume.instrumentation.MonitorService;
import org.apache.flume.instrumentation.MonitoringType;
import org.apache.flume.lifecycle.LifecycleAware;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.lifecycle.LifecycleSupervisor;
import org.apache.flume.lifecycle.LifecycleSupervisor.SupervisorPolicy;
import org.apache.flume.node.net.AuthorizationProvider;
import org.apache.flume.node.net.BasicAuthorizationProvider;
import org.apache.flume.sink.AbstractSingleSinkProcessor;
import org.apache.flume.sink.AbstractSinkProcessor;
import org.apache.flume.util.SSLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

public class Application {

    private static final Logger logger = LoggerFactory
            .getLogger(Application.class);

    public static final String CONF_MONITOR_CLASS = "flume.monitoring.type";
    public static final String CONF_MONITOR_PREFIX = "flume.monitoring.";

    private static final int DEFAULT_INTERVAL = 300;
    private static final int DEFAULT_FILE_INTERVAL = 30;
    private final List<LifecycleAware> components;
    private final LifecycleSupervisor supervisor;
    private MaterializedConfiguration materializedConfiguration;
    private MonitorService monitorServer;
    private final ReentrantLock lifecycleLock = new ReentrantLock();

    public Application() {
        this(new ArrayList<LifecycleAware>(0));
    }

    public Application(List<LifecycleAware> components) {
        this.components = components;
        supervisor = new LifecycleSupervisor();
    }

    public void start() {
        lifecycleLock.lock();
        try {
            for (LifecycleAware component : components) {
                supervisor.supervise(component, new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    @Subscribe
    public void handleConfigurationEvent(MaterializedConfiguration conf) {
        try {
            lifecycleLock.lockInterruptibly();

            // 一般情况下 不执行里面的具体代码
            stopAllComponents();

            // 初始化全部组件 (source、channel、sink 实现 Initializable 接口 一般情况下不执行里面的具体代码)
            initializeAllComponents(conf);

            /**
             * 启动全部组件 (包括监控组件)
             */
            startAllComponents(conf);
        } catch (InterruptedException e) {
            logger.info("Interrupted while trying to handle configuration event");
            return;
        } finally {
            // If interrupted while trying to lock, we don't own the lock, so must not attempt to unlock
            if (lifecycleLock.isHeldByCurrentThread()) {
                lifecycleLock.unlock();
            }
        }
    }

    public void stop() {
        lifecycleLock.lock();
        try {
            stopAllComponents();
            supervisor.stop();
            if (monitorServer != null) {
                monitorServer.stop();
            }
        } finally {
            lifecycleLock.unlock();
        }
    }

    private void stopAllComponents() {
        if (this.materializedConfiguration != null) {
            logger.info("Shutting down configuration: {}", this.materializedConfiguration);
            for (Entry<String, SourceRunner> entry :
                    this.materializedConfiguration.getSourceRunners().entrySet()) {
                try {
                    logger.info("Stopping Source " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    logger.error("Error while stopping {}", entry.getValue(), e);
                }
            }

            for (Entry<String, SinkRunner> entry :
                    this.materializedConfiguration.getSinkRunners().entrySet()) {
                try {
                    logger.info("Stopping Sink " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    logger.error("Error while stopping {}", entry.getValue(), e);
                }
            }

            for (Entry<String, Channel> entry :
                    this.materializedConfiguration.getChannels().entrySet()) {
                try {
                    logger.info("Stopping Channel " + entry.getKey());
                    supervisor.unsupervise(entry.getValue());
                } catch (Exception e) {
                    logger.error("Error while stopping {}", entry.getValue(), e);
                }
            }
        }
        if (monitorServer != null) {
            monitorServer.stop();
        }
    }

    private void initializeAllComponents(MaterializedConfiguration materializedConfiguration) {
        logger.info("Initializing components");
        /**
         * 场景驱动情况下 ch = MemoryChannel 且生命周期初始状态为 IDLE 但是 MemoryChannel 不属于 Initializable 子类
         */
        for (Channel ch : materializedConfiguration.getChannels().values()) {
            while (ch.getLifecycleState() != LifecycleState.START && ch instanceof Initializable) {
                ((Initializable) ch).initialize(materializedConfiguration);
            }
        }

        /**
         * 场景驱动情况下 sinkRunner = SinkRunner
         */
        for (SinkRunner sinkRunner : materializedConfiguration.getSinkRunners().values()) {
            /**
             * 场景驱动情况下 processor = DefaultSinkProcessor (继承于 AbstractSingleSinkProcessor)
             */
            SinkProcessor processor = sinkRunner.getPolicy();
            if (processor instanceof AbstractSingleSinkProcessor) {
                /**
                 * 场景驱动情况下 sink = KafkaSink (不是 Initializable 子类)
                 */
                Sink sink = ((AbstractSingleSinkProcessor) processor).getSink();
                if (sink instanceof Initializable) {
                    ((Initializable) sink).initialize(materializedConfiguration);
                }
            } else if (processor instanceof AbstractSinkProcessor) {
                for (Sink sink : ((AbstractSinkProcessor) processor).getSinks()) {
                    if (sink instanceof Initializable) {
                        ((Initializable) sink).initialize(materializedConfiguration);
                    }
                }
            }
        }

        /**
         * 场景驱动情况下 sourceRunner = EventDrivenSourceRunner
         */
        for (SourceRunner sourceRunner : materializedConfiguration.getSourceRunners().values()) {
            /**
             * 场景驱动情况下 source = SyslogUDPSource (不是 Initializable 子类)
             */
            Source source = sourceRunner.getSource();
            if (source instanceof Initializable) {
                ((Initializable) source).initialize(materializedConfiguration);
            }
        }
    }

    private void startAllComponents(MaterializedConfiguration materializedConfiguration) {
        logger.info("Starting new configuration:{}", materializedConfiguration);

        // 核心配置
        this.materializedConfiguration = materializedConfiguration;

        /**
         * 场景驱动情况下 最终调用 MemoryChannel.start()
         */
        for (Entry<String, Channel> entry : materializedConfiguration.getChannels().entrySet()) {
            try {
                logger.info("Starting Channel " + entry.getKey());
                supervisor.supervise(entry.getValue(), new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
            } catch (Exception e) {
                logger.error("Error while starting {}", entry.getValue(), e);
            }
        }

        /**
         * 等待 Channel 状态为 START
         */
        for (Channel ch : materializedConfiguration.getChannels().values()) {
            while (ch.getLifecycleState() != LifecycleState.START
                    && !supervisor.isComponentInErrorState(ch)) {
                try {
                    logger.info("Waiting for channel: " + ch.getName() +
                            " to start. Sleeping for 500 ms");
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for channel to start.", e);
                    Throwables.propagate(e);
                }
            }
        }

        /**
         * 场景驱动情况下 调用 SinkRunner.start()
         * sink 拉取 channel 数据被封装为 PollingRunner 线程
         * PollingRunner 线程是一个死循环 每次运行一个执行一个事务
         * 每个事务默认从 channel 拉取 100 条数据
         */
        for (Entry<String, SinkRunner> entry : materializedConfiguration.getSinkRunners().entrySet()) {
            try {
                logger.info("Starting Sink " + entry.getKey());
                supervisor.supervise(entry.getValue(), new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
            } catch (Exception e) {
                logger.error("Error while starting {}", entry.getValue(), e);
            }
        }

        /**
         * 场景驱动情况下 调用 EventDrivenSourceRunner.start()
         * 最终调用 SyslogUDPSource.start() 启动 source
         */
        for (Entry<String, SourceRunner> entry : materializedConfiguration.getSourceRunners().entrySet()) {
            try {
                logger.info("Starting Source " + entry.getKey());
                supervisor.supervise(entry.getValue(), new SupervisorPolicy.AlwaysRestartPolicy(), LifecycleState.START);
            } catch (Exception e) {
                logger.error("Error while starting {}", entry.getValue(), e);
            }
        }

        /**
         * 上报 Flume 监控指标信息
         */
        this.loadMonitoring();
    }

    @SuppressWarnings("unchecked")
    private void loadMonitoring() {
        /**
         * 获取系统环境变量
         */
        Properties systemProps = System.getProperties();

        /**
         * 测试监控
         */
        // systemProps.setProperty("flume.monitoring.type", "http");

        Set<String> keys = systemProps.stringPropertyNames();
        try {
            /**
             * 判断系统环境变量是否存在 key = flume.monitoring.type 对应的值
             * 如果想要配置 flume 监控 一般在执行命令的时候添加对应的参数 比如如下:
             * bin/flume-ng agent --conf-file example.conf --name a1 -Dflume.monitoring.type=http -Dflume.monitoring.port=34545
             */
            if (keys.contains(CONF_MONITOR_CLASS)) {
                String monitorType = systemProps.getProperty(CONF_MONITOR_CLASS);
                Class<? extends MonitorService> klass;
                try {
                    //Is it a known type?
                    klass = MonitoringType.valueOf(monitorType.toUpperCase(Locale.ENGLISH)).getMonitorClass();
                } catch (Exception e) {
                    //Not a known type, use FQCN
                    klass = (Class<? extends MonitorService>) Class.forName(monitorType);
                }
                this.monitorServer = klass.getConstructor().newInstance();
                Context context = new Context();
                for (String key : keys) {
                    if (key.startsWith(CONF_MONITOR_PREFIX)) {
                        context.put(key.substring(CONF_MONITOR_PREFIX.length()),
                                systemProps.getProperty(key));
                    }
                }
                monitorServer.configure(context);
                monitorServer.start();
            }
        } catch (ReflectiveOperationException e) {
            logger.warn("Error starting monitoring. "
                    + "Monitoring might not be available.", e);
        }
    }

    // args = -n a1 -f D:\example\flume-netcat-logger.conf
    public static void main(String[] args) {
        // 读取 flume.opts 配置文件
        Properties initProps = loadConfigOpts();

        try {
            SSLUtil.initGlobalSSLParameters();

            Options options = new Options();

            Option option = new Option("n", "name", true, "the name of this agent");
            option.setRequired(true);
            options.addOption(option);

            option = new Option("f", "conf-file", true,
                    "specify a config file (required if -c, -u, and -z are missing)");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("u", "conf-uri", true,
                    "specify a config uri (required if -c, -f and -z are missing)");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("a", "auth-provider", true,
                    "specify an authorization provider class");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("prov", "conf-provider", true,
                    "specify a configuration provider class (required if -f, -u, and -z are missing)");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("user", "conf-user", true, "user name to access configuration uri");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("pwd", "conf-password", true, "password to access configuration uri");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("i", "poll-interval", true,
                    "number of seconds between checks for a configuration change");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("b", "backup-directory", true,
                    "directory in which to store the backup configuration file");
            option.setRequired(false);
            options.addOption(option);

            option = new Option(null, "no-reload-conf", false,
                    "do not reload config file if changed");
            options.addOption(option);

            // Options for Zookeeper
            option = new Option("z", "zkConnString", true,
                    "specify the ZooKeeper connection to use (required if -c, -f, and -u are missing)");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("p", "zkBasePath", true,
                    "specify the base path in ZooKeeper for agent configs");
            option.setRequired(false);
            options.addOption(option);

            option = new Option("h", "help", false, "display help text");
            options.addOption(option);

            DefaultParser parser = new DefaultParser();
            CommandLine commandLine = parser.parse(options, args, initProps);

            if (commandLine.hasOption('h')) {
                new HelpFormatter().printHelp("flume-ng agent", options, true);
                return;
            }

            String agentName = commandLine.getOptionValue('n');
            boolean reload = !commandLine.hasOption("no-reload-conf");

            boolean isZkConfigured = false;
            if (commandLine.hasOption('z') || commandLine.hasOption("zkConnString")) {
                isZkConfigured = true;
            }

            List<URI> confUri = null;
            ConfigurationProvider provider = null;
            int defaultInterval = DEFAULT_FILE_INTERVAL;
            if (commandLine.hasOption('u') || commandLine.hasOption("conf-uri")) {
                confUri = new ArrayList<>();
                for (String uri : commandLine.getOptionValues("conf-uri")) {
                    if (uri.toLowerCase(Locale.ROOT).startsWith("http")) {
                        defaultInterval = DEFAULT_INTERVAL;
                    }
                    confUri.add(new URI(uri));
                }
            } else if (commandLine.hasOption("f") || commandLine.hasOption("conf-file")) {
                confUri = new ArrayList<>();
                // 将配置文件路径添加到 confUri 集合
                for (String filePath : commandLine.getOptionValues("conf-file")) {
                    confUri.add(new File(filePath).toURI());
                }
            }

            if (commandLine.hasOption("prov") || commandLine.hasOption("conf-provider")) {
                String className = commandLine.getOptionValue("conf-provider");
                try {
                    Class<?> clazz = Application.class.getClassLoader().loadClass(className);
                    Constructor<?> constructor = clazz.getConstructor(String[].class);
                    provider = (ConfigurationProvider) constructor.newInstance((Object[]) args);
                } catch (ReflectiveOperationException ex) {
                    logger.error("Error creating ConfigurationProvider {}", className, ex);
                }
            }

            Application application;
            if (provider != null) {
                List<LifecycleAware> components = Lists.newArrayList();
                application = new Application(components);
                application.handleConfigurationEvent(provider.getConfiguration());
            } else if (isZkConfigured) {
                // get options
                String zkConnectionStr = commandLine.getOptionValue('z');
                String baseZkPath = commandLine.getOptionValue('p');

                if (reload) {
                    EventBus eventBus = new EventBus(agentName + "-event-bus");
                    List<LifecycleAware> components = Lists.newArrayList();
                    PollingZooKeeperConfigurationProvider zookeeperConfigurationProvider =
                            new PollingZooKeeperConfigurationProvider(
                                    agentName, zkConnectionStr, baseZkPath, eventBus);
                    components.add(zookeeperConfigurationProvider);
                    application = new Application(components);
                    eventBus.register(application);
                } else {
                    StaticZooKeeperConfigurationProvider zookeeperConfigurationProvider =
                            new StaticZooKeeperConfigurationProvider(
                                    agentName, zkConnectionStr, baseZkPath);
                    application = new Application();
                    application.handleConfigurationEvent(zookeeperConfigurationProvider.getConfiguration());
                }
            } else if (confUri != null) {
                String confUser = commandLine.getOptionValue("conf-user");
                String confPassword = commandLine.getOptionValue("conf-password");
                String pollInterval = commandLine.getOptionValue("poll-interval");
                String backupDirectory = commandLine.getOptionValue("backup-directory");
                int interval = StringUtils.isNotEmpty(pollInterval) ? Integer.parseInt(pollInterval) : 0;
                String verify = commandLine.getOptionValue("verify-host", "true");
                boolean verifyHost = Boolean.parseBoolean(verify);
                AuthorizationProvider authorizationProvider = null;
                String authProviderClass = commandLine.getOptionValue("auth-provider");
                if (authProviderClass != null) {
                    try {
                        Class<?> clazz = Class.forName(authProviderClass);
                        Object obj = clazz.getDeclaredConstructor(String[].class)
                                .newInstance((Object[]) args);
                        if (obj instanceof AuthorizationProvider) {
                            authorizationProvider = (AuthorizationProvider) obj;
                        } else {
                            logger.error(
                                    "The supplied authorization provider does not implement AuthorizationProvider");
                            return;
                        }
                    } catch (ReflectiveOperationException ex) {
                        logger.error("Unable to create authorization provider: {}", ex.getMessage());
                        return;
                    }
                }
                if (authorizationProvider == null && StringUtils.isNotEmpty(confUser)
                        && StringUtils.isNotEmpty(confPassword)) {
                    authorizationProvider = new BasicAuthorizationProvider(confUser, confPassword);
                }
                EventBus eventBus = null;
                // 默认情况下 入口参数没有设置 --no-reload-conf 故 reload = true
                if (reload) {
                    // 创建事件通道
                    eventBus = new EventBus(agentName + "-event-bus");
                    if (interval == 0) {
                        interval = defaultInterval;
                    }
                }
                List<ConfigurationSource> configurationSources = new ArrayList<>();
                // 解析配置文件内容添加到 configurationSources 集合
                for (URI uri : confUri) {
                    ConfigurationSource configurationSource =
                            ConfigurationSourceFactory.getConfigurationSource(uri, authorizationProvider, verifyHost);
                    if (configurationSource != null) {
                        configurationSources.add(configurationSource);
                    }
                }

                List<LifecycleAware> components = Lists.newArrayList();
                // 创建 UriConfigurationProvider (既是配置类, 也是生命周期类)
                /**
                 * 创建 UriConfigurationProvider 会初始化三个核心工厂
                 * 1. DefaultSourceFactory
                 * 2. DefaultSinkFactory
                 * 3. DefaultChannelFactory
                 */
                UriConfigurationProvider configurationProvider = new UriConfigurationProvider(agentName,
                        configurationSources, backupDirectory, eventBus, interval);
                components.add(configurationProvider);

                // 创建 Application 应用并往事件通道注册
                application = new Application(components);
                if (eventBus != null) {
                    eventBus.register(application);
                }

                // 初始化和启动 Source、Channel、Sink 组件
                application.handleConfigurationEvent(
                        // 比较核心操作, 也即解析配置生成对应的对象并将 Source、Channel、Sink 连接在一起 完成整个 Pipeline 的构建
                        /**
                         * 基于场景驱动下
                         * 1. source = SyslogUDPSource
                         * 2. selector = ReplicatingChannelSelector
                         * 3. channel = MemoryChannel
                         * 4. sink = KafkaSink
                         */
                        configurationProvider.getConfiguration()
                );
            } else {
                throw new ParseException("No configuiration was provided");
            }
            /**
             * 启动 Application 本质是监控配置文件是否发送变化
             * 场景驱动情况下 调用 UriConfigurationProvider.start() 一般监控到配置文件被修改则回调
             * Application.handleConfigurationEvent(conf) 因为该方法上面存在 @Subscribe 注解 并且 conf 为最新配置文件内容
             */
            application.start();

            final Application appReference = application;
            Runtime.getRuntime().addShutdownHook(new Thread("agent-shutdown-hook") {
                @Override
                public void run() {
                    appReference.stop();
                }
            });

        } catch (ParseException | URISyntaxException | RuntimeException e) {
            logger.error("A fatal error occurred while running. Exception follows.", e);
        }
    }

    @SuppressWarnings("PMD")
    private static Properties loadConfigOpts() {
        Properties initProps = new Properties();
        InputStream is = null;
        try {
            is = new FileInputStream("/etc/flume/flume.opts");
        } catch (IOException ex) {
            // Ignore the exception.
        }
        if (is == null) {
            is = Application.class.getClassLoader().getResourceAsStream("flume.opts");
        }
        if (is != null) {
            try {
                initProps.load(is);
            } catch (Exception ex) {
                logger.warn("Unable to load options file due to: {}", ex.getMessage());
            } finally {
                try {
                    is.close();
                } catch (IOException ex) {
                    // Ignore this error.
                }
            }
        }
        return initProps;
    }
}