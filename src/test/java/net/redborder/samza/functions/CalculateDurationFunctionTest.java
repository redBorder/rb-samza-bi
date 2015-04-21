package net.redborder.samza.functions;

import junit.framework.TestCase;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;

@RunWith(MockitoJUnitRunner.class)
public class CalculateDurationFunctionTest extends TestCase {
    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    public long secs(DateTime date) {
        return (date.getMillis() / 1000);
    }

    @Test
    public void calculatesDuration() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.parseDateTime("2014-01-01 22:10:50");
        DateTime timestampDate = formatter.parseDateTime("2014-01-01 22:12:27");
        message.put(FIRST_SWITCHED, Long.valueOf(secs(firstSwitchDate)));
        message.put(TIMESTAMP, Long.valueOf(secs(timestampDate)));
        message.put(BYTES, 999);
        message.put(PKTS, 99);

        Map<String, Object> result = CalculateDurationFunction.execute(message);
        assertEquals(97L, result.get(DURATION));
    }

    @Test
    public void calculatesDurationWithoutFirstSwitched() {
        Map<String, Object> message = new HashMap<>();
        DateTime timestampDate = formatter.parseDateTime("2014-01-01 22:12:27");
        message.put(TIMESTAMP, Long.valueOf(secs(timestampDate)));
        message.put(BYTES, 999);
        message.put(PKTS, 99);

        Map<String, Object> result = CalculateDurationFunction.execute(message);
        assertEquals(0L, result.get(DURATION));
    }

    @Test
    public void calculatesDurationWithoutDates() {
        Map<String, Object> message = new HashMap<>();
        message.put(BYTES, 999);
        message.put(PKTS, 99);

        Map<String, Object> result = CalculateDurationFunction.execute(message);
        assertEquals(0L, result.get(DURATION));
    }
}