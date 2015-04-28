/*
 * Copyright (c) 2015 ENEO Tecnologia S.L.
 * This file is part of redBorder.
 * redBorder is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * redBorder is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with redBorder. If not, see <http://www.gnu.org/licenses/>.
 */

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

import static net.redborder.samza.util.constants.Dimension.*;
import static net.redborder.samza.util.constants.Aggregators.*;


public class FlowGoldBeamFactory implements BeamFactory
{
    @Override
    public Beam<Object> makeBeam(SystemStream stream, Config config)
    {
        final int maxRows = Integer.valueOf(config.get("redborder.beam.flow.gold.maxrows", "200000"));
        final int partitions = Integer.valueOf(config.get("redborder.beam.flow.gold.partitions", "2"));
        final int replicas = Integer.valueOf(config.get("redborder.beam.flow.gold.replicas", "1"));
        final String intermediatePersist = config.get("redborder.beam.flow.gold.intermediatePersist", "PT20m");
        final String zkConnect = config.get("systems.kafka.consumer.zookeeper.connect");
        final String dataSource = stream.getStream();

        final List<String> dimensions = ImmutableList.of(
                APPLICATION_ID_NAME, BITFLOW_DIRECTION, CONVERSATION, DIRECTION,
                ENGINE_ID_NAME, HTTP_USER_AGENT_OS, HTTP_HOST, HTTP_SOCIAL_MEDIA,
                HTTP_SOCIAL_USER, HTTP_REFER_L1, L4_PROTO, IP_PROTOCOL_VERSION,
                SENSOR_NAME, SENSOR_IP, SCATTERPLOT, SRC_IP, SRC_COUNTRY_CODE, SRC_NET_NAME,
                SRC_PORT, SRC_AS_NAME, CLIENT_MAC, CLIENT_ID, CLIENT_MAC_VENDOR,
                DOT11STATUS, SRC_VLAN, SRC_MAP, SRV_PORT, DST_IP,
                DST_COUNTRY_CODE, DST_NET_NAME, DST_AS_NAME, DST_PORT,
                DST_VLAN, DST_MAP, INPUT_SNMP, OUTPUT_SNMP, TOS,
                CLIENT_LATLNG, COORDINATES_MAP, CLIENT_CAMPUS,
                CLIENT_BUILDING, CLIENT_FLOOR, WIRELESS_ID, CLIENT_RSSI, CLIENT_RSSI_NUM,
                CLIENT_SNR, CLIENT_SNR_NUM, WIRELESS_STATION, HNBLOCATION, HNBGEOLOCATION, RAT,
                DARKLIST_SCORE_NAME, DARKLIST_CATEGORY, DARKLIST_PROTOCOL,
                DARKLIST_DIRECTION, DARKLIST_SCORE);

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
                .location(DruidLocation.create("overlord", "druid:local:firehose:%s", dataSource))
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