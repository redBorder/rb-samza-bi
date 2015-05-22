package net.redborder.samza.indexing.tranquility;


import com.google.common.collect.ImmutableList;
import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.*;
import com.metamx.tranquility.samza.BeamFactory;
import com.metamx.tranquility.typeclass.Timestamper;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import net.redborder.samza.indexing.autoscaling.AutoScalingUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemStream;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static net.redborder.samza.util.constants.Aggregators.EVENTS_AGGREGATOR;
import static net.redborder.samza.util.constants.Dimension.*;
import static net.redborder.samza.util.constants.Dimension.TIMESTAMP;

public class HashtagBeamFactory implements BeamFactory {

    @Override
    public Beam<Object> makeBeam(SystemStream stream, Config config) {
        final int maxRows = Integer.valueOf(config.get("redborder.beam.state.maxrows", "200000"));
        final String intermediatePersist = config.get("redborder.beam.state.intermediatePersist", "PT20m");
        final String zkConnect = config.get("systems.kafka.consumer.zookeeper.connect");
        final String dataSource = stream.getStream();

        final Integer partitions = AutoScalingUtils.getPartitions(dataSource);
        final Integer replicas = AutoScalingUtils.getReplicas(dataSource);
        final String realDataSource = AutoScalingUtils.getDataSource(dataSource);

        final List<String> dimensions = ImmutableList.of(
                SENSOR_NAME, SENSOR_ID, DEPLOYMENT, DEPLOYMENT_ID, NAMESPACE, NAMESPACE_ID, TYPE, VALUE
        );

        final List<AggregatorFactory> aggregators = Arrays.asList(new AggregatorFactory[] { new CountAggregatorFactory(EVENTS_AGGREGATOR)});

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
                .location(DruidLocation.create("overlord", "druid:local:firehose:%s", realDataSource))
                .rollup(DruidRollup.create(DruidDimensions.specific(dimensions), aggregators, QueryGranularity.MINUTE))
                .druidTuning(DruidTuning.create(maxRows, new Period(intermediatePersist), 0))
                .tuning(ClusteredBeamTuning.builder()
                        .partitions(partitions)
                        .replicants(replicas)
                        .segmentGranularity(Granularity.HOUR)
                        .warmingPeriod(new Period("PT5M"))
                        .windowPeriod(new Period("PT15M"))
                        .build())
                .timestampSpec(new TimestampSpec(TIMESTAMP, "posix"))
                .buildBeam();
    }
}