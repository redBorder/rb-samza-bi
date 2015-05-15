package net.redborder.samza.tranquility;

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
import io.druid.query.aggregation.LongSumAggregatorFactory;
import net.redborder.samza.util.AutoScalingManager;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemStream;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.List;
import java.util.Map;

import static net.redborder.samza.util.constants.Aggregators.EVENTS_AGGREGATOR;
import static net.redborder.samza.util.constants.Aggregators.SIGNATURES_AGGREGATOR;
import static net.redborder.samza.util.constants.Dimension.*;

public class EventBeamFactory implements BeamFactory
{
    @Override
    public Beam<Object> makeBeam(SystemStream stream, Config config)
    {
        final int maxRows = Integer.valueOf(config.get("redborder.beam.event.maxrows", "200000"));
        final String intermediatePersist = config.get("redborder.beam.event.intermediatePersist", "PT20m");
        final String zkConnect = config.get("systems.kafka.consumer.zookeeper.connect");
        final String dataSource = stream.getStream();

        final Integer partitions = AutoScalingManager.getPartitions(dataSource);
        final Integer replicas = AutoScalingManager.getReplicas(dataSource);
        final String realDataSource = AutoScalingManager.getDataSource(dataSource);

        final List<String> dimensions = ImmutableList.of(
            ACTION, CLASSIFICATION, CONVERSATION, DOMAIN_NAME, ETHLENGTH_RANGE,
            GROUP_NAME, SIG_GENERATOR, ICMPTYPE, IPLEN_RANGE, L4_PROTO, REV,
            SENSOR_NAME, SENSOR_ID, DEPLOYMENT, NAMESPACE, NAMESPACE_ID, PRIORITY, MSG, SIG_ID,
            SCATTERPLOT, ETHSRC, ETHSRC_VENDOR, SRC, SRC_COUNTRY_CODE, SRC_NET_NAME,
            SRC_PORT, SRC_AS_NAME, SRC_MAP, ETHDST, ETHDST_VENDOR, DST, DST_COUNTRY_CODE,
            DST_NET_NAME, DST_PORT, DST_AS_NAME, DST_MAP, TOS, TTL, VLAN, DARKLIST_SCORE_NAME,
            DARKLIST_CATEGORY, DARKLIST_PROTOCOL, DARKLIST_DIRECTION, DARKLIST_SCORE,
            MARKET, ORGANIZATION, CLIENT_LATLONG, CLIENT_FLOOR, CLIENT_ZONE, CLIENT_BUILDING,
            CLIENT_CAMPUS, WIRELESS_STATION, SHA256, FILE_SIZE, FILE_URI, FILE_HOSTNAME
        );

        final List<AggregatorFactory> aggregators = ImmutableList.of(
            new CountAggregatorFactory(EVENTS_AGGREGATOR),
            new LongSumAggregatorFactory(SIGNATURES_AGGREGATOR, MSG)
            // TODO: if @darklist_enable => {"type":"longSum", "fieldName":"darklist_score", "name":"sum_dl_score"}
        );

        // The Timestamper should return the timestamp of the class your Samza task produces. Samza envelopes contain
        // Objects, so you'll generally have to cast them here.
        final Timestamper<Object> timestamper = new Timestamper<Object>()
        {
            @Override
            public DateTime timestamp(Object obj)
            {
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