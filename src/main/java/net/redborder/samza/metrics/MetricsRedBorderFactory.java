package net.redborder.samza.metrics;

import org.apache.samza.config.*;
import org.apache.samza.metrics.*;
import org.apache.samza.metrics.reporter.Metrics;
import org.apache.samza.metrics.reporter.MetricsHeader;
import org.apache.samza.metrics.reporter.MetricsSnapshot;
import org.apache.samza.metrics.reporter.MetricsSnapshotReporter;
import org.apache.samza.serializers.Serde;
import org.apache.samza.serializers.SerdeFactory;
import org.apache.samza.serializers.Serializer;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemFactory;
import org.apache.samza.system.SystemProducer;
import org.apache.samza.system.SystemStream;
import org.apache.samza.util.DaemonThreadFactory;
import org.apache.samza.util.Util;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class MetricsRedBorderFactory implements MetricsReporterFactory {

    @Override
    public MetricsReporter getMetricsReporter(String name, String containerName, Config config) {
        JobConfig jobConfig = new JobConfig(config);
        TaskConfig taskConfig = new TaskConfig(config);
        MetricsConfig metricsConfig = new MetricsConfig(config);
        SystemConfig systemConfig = new SystemConfig(config);
        SerializerConfig serializerConfig = new SerializerConfig(config);
        StreamConfig streamConfig = new StreamConfig(config);
        String jobName = jobConfig.getName().get();

        String jobId;
        try {
            jobId = jobConfig.getJobId().get();
        } catch (NoSuchElementException e) {
            jobId = "1";
        }

        String taskClass = taskConfig.getTaskClass().get();

        String metricsSystemStreamName = metricsConfig.getMetricsReporterStream(name).get();

        SystemStream systemStream = Util.getSystemStreamFromNames(metricsSystemStreamName);

        String systemName = systemStream.getSystem();
        String systemFactoryClassName = systemConfig.getSystemFactory(systemName).get();

        ReadableMetricsRegistry registry = new MetricsRegistryMap();

        SystemFactory systemFactory = Util.getObj(systemFactoryClassName);
        SystemProducer producer = systemFactory.getProducer(systemName, config, registry);

        String streamSerdeName = null;
        try {
            streamSerdeName = streamConfig.getStreamMsgSerde(systemStream).get();
        } catch (NoSuchElementException e) { }

        String systemSerdeName = null;
        try {
            systemSerdeName = systemConfig.getSystemMsgSerde(systemName).get();
        } catch (NoSuchElementException e) { }


        String serdeName;
        if (streamSerdeName != null) {
            serdeName = streamSerdeName;
        } else {
            serdeName = systemSerdeName;
        }

        Serde<MetricsSnapshot> serde = null;

        if(serdeName != null){
           String serdeClass =  serializerConfig.getSerdeClass(serdeName).get();
            SerdeFactory<MetricsSnapshot> serdeFactory = (SerdeFactory<MetricsSnapshot>) Util.getObj(serdeClass);
            serde = serdeFactory.getSerde(serdeName, config);
        }

        MetricsReporter reporter = new MetricsReporterRB(producer, systemStream, jobName, jobId, taskClass, containerName, serde);
        reporter.register(this.getClass().getSimpleName().toString(), registry);

        return reporter;
    }


    private class MetricsReporterRB implements MetricsReporter, Runnable {

        private SystemProducer producer;
        private String jobName;
        private String jobId;
        private String taskClass;
        private String containerName;
        private Map<String, ReadableMetricsRegistry> registries;
        private Long resetTime;
        private Serializer<MetricsSnapshot> serializer;
        private SystemStream systemStream;

        private ScheduledExecutorService executor;

        public MetricsReporterRB(SystemProducer producer, SystemStream systemStream, String jobName, String jobId, String taskClass, String containerName, Serializer<MetricsSnapshot> serializer) {
            this.producer = producer;
            this.jobName = jobName;
            this.systemStream = systemStream;
            this.jobId = jobId;
            this.taskClass = taskClass;
            this.containerName = containerName;
            this.registries = new HashMap<>();
            this.resetTime = System.currentTimeMillis();
            this.serializer = serializer;
            this.executor = Executors.newScheduledThreadPool(1, new DaemonThreadFactory(MetricsSnapshotReporter.METRIC_SNAPSHOT_REPORTER_THREAD_NAME_PREFIX()));
        }

        @Override
        public void start() {
            this.producer.start();
            this.executor.scheduleWithFixedDelay(this, 0, 60, TimeUnit.SECONDS);
        }

        @Override
        public void register(String source, ReadableMetricsRegistry registry) {
            registries.put(source, registry);
            producer.register(source);
        }

        @Override
        public void stop() {
            producer.stop();
            executor.shutdown();
            try {
                executor.awaitTermination(60, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }

        @Override
        public void run() {
            for (Map.Entry<String, ReadableMetricsRegistry> registry : registries.entrySet()) {
                Map<String, Map<String, Object>> metricsMsg = new HashMap<>();

                for(String group : registry.getValue().getGroups()){
                    final Map<String, Object> groupMsg = new HashMap<>();

                    for(final Map.Entry<String, Metric> entry : registry.getValue().getGroup(group).entrySet()){
                        entry.getValue().visit(new MetricsVisitor() {
                            @Override
                            public void counter(Counter counter) {
                                groupMsg.put(entry.getKey(), counter.getCount());
                                counter.clear();
                            }

                            @Override
                            public <T> void gauge(Gauge<T> gauge) {
                                groupMsg.put(entry.getKey(), gauge.getValue());
                            }

                            @Override
                            public void timer(Timer timer) {
                                groupMsg.put(entry.getKey(), timer.getSnapshot().getAverage());
                            }
                        });
                    }
                    metricsMsg.put(group, groupMsg);
                }

                String host = null;
                try {
                     host = InetAddress.getLocalHost().getHostName();
                } catch (UnknownHostException e) {
                    e.printStackTrace();
                }

                MetricsSnapshot metricsSnapshot = new MetricsSnapshot(new MetricsHeader(jobName, jobId, containerName, registry.getKey(), "", "", host, System.currentTimeMillis(), resetTime)
                        , new Metrics(metricsMsg));

                if(serializer != null){
                    producer.send(registry.getKey(), new OutgoingMessageEnvelope(systemStream, host, null, serializer.toBytes(metricsSnapshot)));
                }else {
                    producer.send(registry.getKey(), new OutgoingMessageEnvelope(systemStream, host, null, metricsSnapshot));
                }

                producer.flush(registry.getKey());
            }
        }
    }
}
