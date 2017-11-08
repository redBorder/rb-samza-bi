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
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemStream;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static net.redborder.samza.util.constants.Aggregators.*;
import static net.redborder.samza.util.constants.Dimension.*;
import static net.redborder.samza.util.constants.Dimension.TIMESTAMP;

public class VaultBeamFactory implements BeamFactory {

    @Override
    public Beam<Object> makeBeam(SystemStream stream, int partitions, int replicas, Config config) {
        final int maxRows = Integer.valueOf(config.get("redborder.beam.vault.maxrows", "200000"));
        final String intermediatePersist = config.get("redborder.beam.vault.intermediatePersist", "PT20m");
        final String zkConnect = config.get("systems.kafka.consumer.zookeeper.connect");
        final long indexGranularity = Long.valueOf(config.get("systems.druid_vault.beam.indexGranularity", "60000"));

        final String dataSource = stream.getStream();

        final List<String> dimensions = ImmutableList.of(
                PRI, PRI_TEXT, SYSLOG_FACILITY, SYSLOG_FACILITY_TEXT, SYSLOGSEVERITY, SYSLOGSEVERITY_TEXT, HOSTNAME,
                FROMHOST_IP, APP_NAME, SENSOR_NAME, PROXY_UUID, MESSAGE, STATUS, CATEGORY, SOURCE, TARGET,
                SENSOR_UUID, SERVICE_PROVIDER_UUID, NAMESPACE_UUID, DEPLOYMENT_UUID, MARKET_UUID, ORGANIZATION_UUID, CAMPUS_UUID,
                BUILDING_UUID, FLOOR_UUID, ACTION
                );

        final List<AggregatorFactory> aggregators =
                Arrays.asList(new AggregatorFactory[]{new CountAggregatorFactory(EVENTS_AGGREGATOR)});

        // The Timestamper should return the timestamp of the class your Samza task produces. Samza envelopes contain
        // Objects, so you'll generally have to cast them here.
        final Timestamper<Object> timestamper = new Timestamper<Object>() {
            @Override
            public DateTime timestamp(Object obj) {
                final Map<String, Object> theMap = (Map<String, Object>) obj;
                Long date = Long.parseLong(theMap.get(TIMESTAMP).toString());
                date = date * 1000;
                return new DateTime(date.longValue());
            }
        };

        final CuratorFramework curator = CuratorFrameworkFactory.builder()
                .connectString(zkConnect)
                .retryPolicy(new ExponentialBackoffRetry(500, 15, 10000))
                .build();

        curator.start();

        return DruidBeams
                .builder(timestamper)
                .curator(curator)
                .discoveryPath("/druid/discoveryPath")
                .location(DruidLocation.create("overlord", "druid:local:firehose:%s", dataSource))
                .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), aggregators, new DurationGranularity(indexGranularity, 0)))
                .druidTuning(DruidTuning.create(maxRows, new Period(intermediatePersist), 0))
                .tuning(ClusteredBeamTuning.builder()
                        .partitions(partitions)
                        .replicants(replicas)
                        .segmentGranularity(Granularity.HOUR)
                        .warmingPeriod(new Period("PT15M"))
                        .windowPeriod(new Period("PT20M"))
                        .build())
                .timestampSpec(new TimestampSpec(TIMESTAMP, "posix", null))
                .buildBeam();
    }

}
