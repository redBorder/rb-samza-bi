package net.redborder.samza.indexing.tranquility;

import com.google.common.collect.ImmutableList;
import com.metamx.common.Granularity;
import com.metamx.tranquility.beam.Beam;
import com.metamx.tranquility.beam.ClusteredBeamTuning;
import com.metamx.tranquility.druid.*;
import com.metamx.tranquility.partition.Partitioner;
import com.metamx.tranquility.samza.BeamFactory;
import com.metamx.tranquility.typeclass.Timestamper;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.DurationGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
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

import static net.redborder.samza.util.constants.Aggregators.*;
import static net.redborder.samza.util.constants.Dimension.*;

public class FlowBeamFactory implements BeamFactory {
    private static final Logger log = LoggerFactory.getLogger(FlowBeamFactory.class);

    @Override
    public Beam<Object> makeBeam(SystemStream stream, Config config) {
        final int maxRows = Integer.valueOf(config.get("redborder.beam.flow.maxrows", "200000"));
        final String intermediatePersist = config.get("redborder.beam.flow.intermediatePersist", "pt20m");
        final String zkConnect = config.get("systems.kafka.consumer.zookeeper.connect");
        final long indexGranularity = Long.valueOf(config.get("systems.druid_flow.beam.indexGranularity", "60000"));

        final String dataSource = stream.getStream();
        final Integer partitions = 1;
        final Integer replicas = 1;

        final List<String> dimensions = ImmutableList.of(
                APPLICATION_ID_NAME, BIFLOW_DIRECTION, CONVERSATION, DIRECTION,
                ENGINE_ID_NAME, HTTP_USER_AGENT_OS, HTTP_HOST, HTTP_SOCIAL_MEDIA,
                HTTP_SOCIAL_USER, HTTP_REFER_L1, L4_PROTO, IP_PROTOCOL_VERSION,
                SENSOR_NAME, SENSOR_UUID, DEPLOYMENT, DEPLOYMENT_UUID, NAMESPACE, NAMESPACE_UUID, SENSOR_IP, SCATTERPLOT,
                SRC_IP, SRC_COUNTRY_CODE, SRC_NET_NAME, SRC_PORT, SRC_AS_NAME, CLIENT_MAC, CLIENT_ID, CLIENT_MAC_VENDOR,
                DOT11STATUS, SRC_VLAN, SRC_MAP, SRV_PORT, DST_IP,
                DST_COUNTRY_CODE, DST_NET_NAME, DST_AS_NAME, DST_PORT,
                DST_VLAN, DST_MAP, INPUT_SNMP, OUTPUT_SNMP, TOS,
                CLIENT_LATLNG, COORDINATES_MAP, CAMPUS, CAMPUS_UUID,
                BUILDING, BUILDING_UUID, FLOOR, FLOOR_UUID, ZONE, WIRELESS_ID,
                CLIENT_RSSI, CLIENT_RSSI_NUM, CLIENT_SNR, CLIENT_SNR_NUM, WIRELESS_STATION, HNBLOCATION, HNBGEOLOCATION,
                RAT, DARKLIST_SCORE_NAME, DARKLIST_CATEGORY, DARKLIST_PROTOCOL, DARKLIST_DIRECTION, DARKLIST_SCORE,
                MARKET, MARKET_UUID, ORGANIZATION, ORGANIZATION_UUID, TYPE, DURATION, DOT11PROTOCOL, INPUT_VRF, OUTPUT_VRF,
                SERVICE_PROVIDER, SERVICE_PROVIDER_UUID, ZONE_UUID, HTTPS_COMMON_NAME, WIRELESS_OPERATOR, CLIENT_NAME,
                TARGET_NAME, CLIENT_ACCOUNTING_TYPE, INTERFACE_NAME, CLIENT_FULLNAME, CLIENT_GENDER, CLIENT_AUTH_TYPE,
                WINDOW);

        final List<AggregatorFactory> aggregators = ImmutableList.of(
                new CountAggregatorFactory(EVENTS_AGGREGATOR),
                new LongSumAggregatorFactory(SUM_BYTES_AGGREGATOR, BYTES),
                new LongSumAggregatorFactory(SUM_PKTS_AGGREGATOR, PKTS),
                new LongSumAggregatorFactory(SUM_RSSI_AGGREGATOR, CLIENT_RSSI_NUM),
                new LongSumAggregatorFactory(SUM_DL_SCORE_AGGREGATOR, DARKLIST_SCORE),
                new HyperUniquesAggregatorFactory(CLIENTS_AGGREGATOR, CLIENT_MAC),
                new HyperUniquesAggregatorFactory(WIRELESS_STATIONS_AGGREGATOR, WIRELESS_STATION)
        );

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
                .curator(curator).partitioner(new Partitioner(){
                    @Override
                    public int partition(Object o, int numPartitions) {
                        return 0;
                    }
                })
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
