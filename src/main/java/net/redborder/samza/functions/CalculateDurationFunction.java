package net.redborder.samza.functions;

import net.redborder.samza.util.constants.Dimension;

import java.util.Map;

public class CalculateDurationFunction {

    public static Map<String, Object> execute(Map<String, Object> event) {
        Object timestamp = event.get(Dimension.TIMESTAMP);
        Object first_switched = event.get(Dimension.FIRST_SWITCHED);

        Long packet_end;
        Long packet_start;

        if (timestamp != null) {
            packet_end = Long.parseLong(timestamp.toString());
        } else {
            packet_end = (System.currentTimeMillis() / 1000);
        }

        if (first_switched != null) {
            packet_start = Long.parseLong(first_switched.toString());
        } else {
            packet_start = packet_end;
        }

        Long duration =  packet_end - packet_start;

        if(duration < 0){
            duration = 1L;
        }

        event.put(Dimension.DURATION, duration);

        return event;
    }
}
