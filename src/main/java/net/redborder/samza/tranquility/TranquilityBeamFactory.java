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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.samza.config.Config;
import org.apache.samza.system.SystemStream;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.util.List;
import java.util.Map;

public class TranquilityBeamFactory implements BeamFactory
{
    @Override
    public Beam<Object> makeBeam(SystemStream stream, Config config)
    {
        final int maxRows = 20000;
        final int partitions = 2;
        final int replicas = 1;
        final String zkConnect = "samza01:2181";
        final String dataSource = stream.getStream();

        final List<String> dimensions = ImmutableList.of(
                "application_id_name", "biflow_direction", "conversation", "direction",
                "engine_id_name", "http_user_agent_os", "http_host", "http_social_media",
                "http_social_user", "http_referer_l1", "l4_proto", "ip_protocol_version",
                "sensor_name", "scatterplot", "src", "src_country_code", "src_net_name",
                "src_port", "src_as_name", "client_mac", "client_id", "client_mac_vendor",
                "dot11_status", "src_vlan", "src_map", "srv_port", "dst",
                "dst_country_code", "dst_net_name", "dst_port", "dst_as_name",
                "dst_vlan", "dst_map", "input_snmp", "output_snmp", "tos",
                "client_latlong", "coordinates_map", "client_campus",
                "client_building", "client_floor", "wireless_id","client_rssi", "client_rssi_num",
                "client_snr", "client_snr_num", "wireless_station", "hnblocation", "hnbgeolocation", "rat",
                "darklist_score_name", "darklist_category", "darklist_protocol",
                "darklist_direction", "darklist_score");

        final List<AggregatorFactory> aggregators = ImmutableList.<AggregatorFactory>of(
                new CountAggregatorFactory("events"),
                new LongSumAggregatorFactory("sum_bytes", "bytes"),
                new LongSumAggregatorFactory("sum_pkts", "pkts")
        );

        // The Timestamper should return the timestamp of the class your Samza task produces. Samza envelopes contain
        // Objects, so you'll generally have to cast them here.
        final Timestamper<Object> timestamper = new Timestamper<Object>()
        {
            @Override
            public DateTime timestamp(Object obj)
            {
                final Map<String, Object> theMap = (Map<String, Object>) obj;
                Long date = Long.parseLong(theMap.get("timestamp").toString());
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
                .druidTuning(DruidTuning.create(maxRows, new Period("PT10M"), 3))
                .tuning(ClusteredBeamTuning.builder()
                        .partitions(partitions)
                        .replicants(replicas)
                        .segmentGranularity(Granularity.HOUR)
                        .warmingPeriod(new Period("PT0M"))
                        .windowPeriod(new Period("PT15M"))
                        .build())
                .timestampSpec(new TimestampSpec("timestamp", "posix"))
                .buildBeam();
    }
}