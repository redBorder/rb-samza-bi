package net.redborder.samza.indexing.tranquility;

import com.google.common.collect.ImmutableList;
import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.*;
import com.metamx.tranquility.samza.BeamFactory;
import com.metamx.tranquility.typeclass.Timestamper;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.DurationGranularity;
import io.druid.query.aggregation.*;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemStream;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class AppMobileSignalBeamFactory
        implements BeamFactory
{
    private static final Logger log = LoggerFactory.getLogger(AppMobileSignalBeamFactory.class);

    public Beam<Object> makeBeam(SystemStream stream, int partitions, int replicas, Config config)
    {
        int maxRows = Integer.valueOf(config.get("redborder.beam.appmobile_signal.maxrows", "200000")).intValue();
        String intermediatePersist = config.get("redborder.beam.appmobile_signal.intermediatePersist", "PT20m");
        String zkConnect = config.get("systems.kafka.consumer.zookeeper.connect");
        long indexGranularity = Long.valueOf(config.get("systems.druid_appmobile_signal.beam.indexGranularity", "60000")).longValue();

        String dataSource = stream.getStream();

        List<String> dimensions = ImmutableList.of("antennaType", "timezone", "IMEI", "CelIdentity", "countryCode", "mobileNetworkCode");

        List<AggregatorFactory> aggregators = ImmutableList.of(
                new DoubleMinAggregatorFactory("min_signal_strength", "signalStrength"),
                new DoubleMaxAggregatorFactory("max_signal_strength", "signalStrength"),
                new DoubleSumAggregatorFactory("sum_signal_strength", "signalStrength"),
                new CountAggregatorFactory("events")
                );

        Timestamper<Object> timestamper = new Timestamper()
        {
            public DateTime timestamp(Object obj)
            {
                Map<String, Object> theMap = (Map)obj;
                Long date = Long.valueOf(Long.parseLong(theMap.get("timestamp").toString()));
                date = Long.valueOf(date.longValue() * 1000L);
                return new DateTime(date.longValue());
            }
        };
        CuratorFramework curator = CuratorFrameworkFactory.builder().connectString(zkConnect).retryPolicy(new ExponentialBackoffRetry(500, 15, 10000)).build();

        curator.start();

        return
                DruidBeams.builder(timestamper)
                        .curator(curator)
                        .discoveryPath("/druid/discoveryPath")
                        .location(DruidLocation.create("overlord", "druid:local:firehose:%s", dataSource))
                        .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), aggregators, new DurationGranularity(indexGranularity, 0L)))
                        .druidTuning(DruidTuning.create(maxRows, new Period(intermediatePersist), 0))
                        .tuning(ClusteredBeamTuning.builder()
                                .partitions(partitions)
                                .replicants(replicas)
                                .segmentGranularity(Granularity.HOUR)
                                .warmingPeriod(new Period("PT15M"))
                                .windowPeriod(new Period("PT10M"))
                                .build())
                        .timestampSpec(new TimestampSpec("timestamp", "posix", null))
                        .buildBeam();
    }
}