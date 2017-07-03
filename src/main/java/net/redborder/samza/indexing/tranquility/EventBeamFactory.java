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
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
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

public class EventBeamFactory implements BeamFactory {
    @Override
    public Beam<Object> makeBeam(SystemStream stream, int partitions, int replicas, Config config) {
        {
            final int maxRows = Integer.valueOf(config.get("redborder.beam.event.maxrows", "200000"));
            final String intermediatePersist = config.get("redborder.beam.event.intermediatePersist", "PT20m");
            final String zkConnect = config.get("systems.kafka.consumer.zookeeper.connect");
            final long indexGranularity = Long.valueOf(config.get("systems.druid_event.beam.indexGranularity", "60000"));

            final String dataSource = stream.getStream();

            final List<String> dimensions = ImmutableList.of(
                    SRC, DST, SRC_PORT, DST_PORT, SRC_AS_NAME, SRC_COUNTRY_CODE, DST_MAP, SRC_MAP, SERVICE_PROVIDER,
                    SHA256, FILE_URI, FILE_SIZE, FILE_HOSTNAME, ACTION, ETHLENGTH_RANGE, ICMPTYPE, ETHSRC, ETHSRC_VENDOR,
                    ETHDST, ETHDST_VENDOR, TTL, VLAN, CLASSIFICATION, DOMAIN_NAME, GROUP_NAME, SIG_GENERATOR, REV, PRIORITY,
                    MSG, SIG_ID, DST_COUNTRY_CODE, DST_AS_NAME, NAMESPACE, DEPLOYMENT, MARKET, ORGANIZATION, CAMPUS, BUILDING,
                    FLOOR, FLOOR_UUID, CONVERSATION, IPLEN_RANGE, L4_PROTO, SENSOR_NAME, SCATTERPLOT, SRC_NET_NAME, DST_NET_NAME,
                    TOS, SERVICE_PROVIDER_UUID, NAMESPACE_UUID, MARKET_UUID, ORGANIZATION_UUID, CAMPUS_UUID, BUILDING_UUID
            );

            final List<AggregatorFactory> aggregators = ImmutableList.of(
                    new CountAggregatorFactory(EVENTS_AGGREGATOR),
                    new HyperUniquesAggregatorFactory(SIGNATURES_AGGREGATOR, MSG)
                    // TODO: if @darklist_enable => {"type":"longSum", "fieldName":"darklist_score", "name":"sum_dl_score"}
            );

            // The Timestamper should return the timestamp of the class your Samza task produces. Samza envelopes contain
            // Objects, so you'll generally have to cast them here.
            final Timestamper<Object> timestamper = new Timestamper<Object>() {
                @Override
                public DateTime timestamp(Object obj) {
                    final Map<String, Object> theMap = (Map<String, Object>) obj;
                    Long date;

                    if(theMap.containsKey(TIMESTAMP)) {
                        date = Long.parseLong(theMap.get(TIMESTAMP).toString()) * 1000;
                    } else {
                        date = System.currentTimeMillis();
                    }
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
                            .windowPeriod(new Period("PT10M"))
                            .build())
                    .timestampSpec(new TimestampSpec(TIMESTAMP, "posix", null))
                    .buildBeam();
        }
    }
}