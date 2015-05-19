package net.redborder.samza.functions;

import net.redborder.samza.util.constants.Dimension;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SplitFlowFunction {
    private static final Logger log = LoggerFactory.getLogger(SplitFlowFunction.class);
    public static int DELAYED_REALTIME_TIME = 15;
    public static long logMark = System.currentTimeMillis();

    public static void warnWithTime(String msg, Object... objs) {
        if ((logMark + 300000) > System.currentTimeMillis()) {
            log.warn(msg, objs);
            logMark = System.currentTimeMillis() + 300000;
        }
    }

    public static List<Map<String, Object>> split(Map<String, Object> event) {
        return split(event, new DateTime());
    }

    public static List<Map<String, Object>> split(Map<String, Object> event, DateTime now) {
        List<Map<String, Object>> generatedPackets = new ArrayList<>();

        // last_switched is timestamp now
        if (event.containsKey(Dimension.FIRST_SWITCHED) && event.containsKey(Dimension.TIMESTAMP)) {
            DateTime packet_start = new DateTime(Long.parseLong(event.get(Dimension.FIRST_SWITCHED).toString()) * 1000);
            DateTime packet_end = new DateTime(Long.parseLong(event.get(Dimension.TIMESTAMP).toString()) * 1000);
            int now_hour = now.getHourOfDay();
            int packet_end_hour = packet_end.getHourOfDay();

            // Get the lower limit date time that a packet can have
            DateTime limit;
            if (now.getMinuteOfHour() < DELAYED_REALTIME_TIME) {
                limit = now.minusHours(1).withMinuteOfHour(0);
            } else {
                limit = now.withMinuteOfHour(0);
            }

            // Discard too old events
            if ((packet_end_hour == now_hour - 1 && now.getMinuteOfHour() > DELAYED_REALTIME_TIME) ||
                    (now.getMillis() - packet_end.getMillis() > 1000 * 60 * 60)) {
                warnWithTime("Dropped packet {} because its realtime processor is already shutdown.", event);
                return generatedPackets;
            } else if (packet_start.isBefore(limit)) {
                // If the lower limit date time is overpassed, correct it
                warnWithTime("Packet {} first switched was corrected because it overpassed the lower limit (event too old).", event);
                packet_start = limit;
                event.put(Dimension.FIRST_SWITCHED, limit.getMillis() / 1000);
            }

            // Correct events in the future
            if (packet_end.isAfter(now) && ((packet_end.getHourOfDay() != packet_start.getHourOfDay()) ||
                    (packet_end.getMillis() - now.getMillis() > 1000 * 60 * 60))) {

                warnWithTime("Packet {} ended in a future segment and I modified its last and/or first switched values.", event);
                event.put(Dimension.TIMESTAMP, now.getMillis() / 1000);
                packet_end = now;

                if (!packet_end.isAfter(packet_start)) {
                    event.put(Dimension.FIRST_SWITCHED, now.getMillis() / 1000);
                    packet_start = now;
                }
            }

            DateTime this_start;
            DateTime this_end = packet_start;

            long bytes = 0;
            long pkts = 0;

            try {
                if (event.containsKey(Dimension.BYTES))
                    bytes = Long.parseLong(event.get(Dimension.BYTES).toString());
            } catch (NumberFormatException e) {
                log.warn("Invalid number of bytes in packet {}.", event);
                return generatedPackets;
            }

            try {
                if (event.containsKey(Dimension.PKTS))
                    pkts = Long.parseLong(event.get(Dimension.PKTS).toString());
            } catch (NumberFormatException e) {
                log.warn("Invalid number of packets in packet {}.", event);
                return generatedPackets;
            }

            long totalDiff = Seconds.secondsBetween(packet_start, packet_end).getSeconds();
            long diff, this_bytes, this_pkts;
            long bytes_count = 0;
            long pkts_count = 0;

            do {
                this_start = this_end;
                this_end = this_start.plusSeconds(60 - this_start.getSecondOfMinute());
                if (this_end.isAfter(packet_end)) this_end = packet_end;
                diff = Seconds.secondsBetween(this_start, this_end).getSeconds();

                if (totalDiff == 0) this_bytes = bytes;
                else this_bytes = (long) Math.ceil(bytes * diff / totalDiff);

                if (totalDiff == 0) this_pkts = pkts;
                else this_pkts = (long) Math.ceil(pkts * diff / totalDiff);

                bytes_count += this_bytes;
                pkts_count += this_pkts;

                Map<String, Object> to_send = new HashMap<>();
                to_send.putAll(event);
                to_send.put(Dimension.TIMESTAMP, this_end.getMillis() / 1000);
                to_send.put(Dimension.BYTES, this_bytes);
                to_send.put(Dimension.PKTS, this_pkts);
                to_send.remove(Dimension.FIRST_SWITCHED);
                generatedPackets.add(to_send);
            } while (this_end.isBefore(packet_end));

            if (bytes != bytes_count || pkts != pkts_count) {
                int last_index = generatedPackets.size() - 1;
                Map<String, Object> last = generatedPackets.get(last_index);
                long new_pkts = ((long) last.get(Dimension.PKTS)) + (pkts - pkts_count);
                long new_bytes = ((long) last.get(Dimension.BYTES)) + (bytes - bytes_count);

                if (new_pkts > 0) last.put(Dimension.PKTS, new_pkts);
                if (new_bytes > 0) last.put(Dimension.BYTES, new_bytes);

                generatedPackets.set(last_index, last);
            }
        } else if (event.containsKey(Dimension.TIMESTAMP)) {
            Long bytes = Long.parseLong(event.get(Dimension.BYTES).toString());
            event.put(Dimension.BYTES, bytes);
            generatedPackets.add(event);
        } else {
            Long bytes = Long.parseLong(event.get(Dimension.BYTES).toString());
            event.put(Dimension.BYTES, bytes);
            log.warn("Packet without timestamp -> {}.", event);
            event.put(Dimension.TIMESTAMP, now.getMillis() / 1000);
            generatedPackets.add(event);
        }

        // We will leave the duration of the message only on the first generated packet
        for (Map<String, Object> packet : generatedPackets) {
            if (generatedPackets.indexOf(packet) == 0) continue;
            packet.remove(Dimension.DURATION);
        }

        return generatedPackets;
    }
}
