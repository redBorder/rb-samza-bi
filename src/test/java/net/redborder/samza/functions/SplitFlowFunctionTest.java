package net.redborder.samza.functions;

import junit.framework.TestCase;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;

@RunWith(MockitoJUnitRunner.class)
public class SplitFlowFunctionTest extends TestCase {
    public DateTime currentTime = new DateTime();
    DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

    public long secs(DateTime date) {
        return (date.getMillis() / 1000);
    }

    @Test
    public void withoutFirstSwitched() {
        Map<String, Object> message = new HashMap<>();
        message.put(TIMESTAMP, secs(currentTime));
        message.put(BYTES, 999);
        message.put(PKTS, 99);

        List<Map<String, Object>> result = SplitFlowFunction.split(message);
        assertEquals(message, result.get(0));
    }

    @Test
    public void withoutTimestamp() {
        Map<String, Object> message = new HashMap<>();
        message.put(BYTES, 999);
        message.put(PKTS, 99);

        List<Map<String, Object>> result = SplitFlowFunction.split(message);
        message.put(TIMESTAMP, secs(currentTime));
        assertEquals(message, result.get(0));
    }

    @Test
    public void lessThanAMinute() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:10:12");
        DateTime timestampDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:10:42");
        DateTime timeNowDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:15:16");

        message.put(TIMESTAMP, secs(timestampDate));
        message.put(FIRST_SWITCHED, secs(firstSwitchDate));
        message.put(BYTES, 999);
        message.put(PKTS, 99);

        Map<String, Object> expected = new HashMap<>();
        expected.put(TIMESTAMP, secs(timestampDate));
        expected.put(BYTES, 999L);
        expected.put(PKTS, 99L);

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertEquals(expected, result.get(0));
    }

    @Test
    public void higherThanAMinute() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:10:12");
        DateTime timestampDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:13:42");
        DateTime timeNowDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:15:16");

        message.put(TIMESTAMP, secs(timestampDate));
        message.put(FIRST_SWITCHED, secs(firstSwitchDate));
        message.put(BYTES, 999);
        message.put(PKTS, 99);

        List<Map<String, Object>> expectedPackets = new ArrayList<>();
        Map<String, Object> expected;
        DateTime pktTime;

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2014-01-01 22:11:00");
        expected.put(TIMESTAMP, secs(pktTime));
        expected.put(BYTES, 228L);
        expected.put(PKTS, 22L);
        expectedPackets.add(expected);

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2014-01-01 22:12:00");
        expected.put(TIMESTAMP, secs(pktTime));
        expected.put(BYTES, 285L);
        expected.put(PKTS, 28L);
        expectedPackets.add(expected);

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2014-01-01 22:13:00");
        expected.put(TIMESTAMP, secs(pktTime));
        expected.put(BYTES, 285L);
        expected.put(PKTS, 28L);
        expectedPackets.add(expected);

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2014-01-01 22:13:42");
        expected.put(TIMESTAMP, secs(pktTime));
        expected.put(BYTES, 201L);
        expected.put(PKTS, 21L);
        expectedPackets.add(expected);

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertEquals(expectedPackets, result);
    }

    @Test
    public void correctsFirstSwitchedMessages() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.withZoneUTC().parseDateTime("2014-01-01 21:58:12");
        DateTime timestampDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:01:42");
        DateTime timeNowDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:22:16");

        message.put(TIMESTAMP, secs(timestampDate));
        message.put(FIRST_SWITCHED, secs(firstSwitchDate));
        message.put(BYTES, 999);
        message.put(PKTS, 99);

        List<Map<String, Object>> expectedPackets = new ArrayList<>();
        Map<String, Object> expected;
        DateTime pktTime;

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2014-01-01 22:01:00");
        expected.put(TIMESTAMP, secs(pktTime));
        expected.put(BYTES, 511L);
        expected.put(PKTS, 50L);
        expectedPackets.add(expected);

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2014-01-01 22:01:42");
        expected.put(TIMESTAMP, secs(pktTime));
        expected.put(BYTES, 488L);
        expected.put(PKTS, 49L);
        expectedPackets.add(expected);

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertEquals(expectedPackets, result);
    }

    @Test
    public void notDropOldMessages() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.withZoneUTC().parseDateTime("2014-01-01 21:50:12");
        DateTime timestampDate = formatter.withZoneUTC().parseDateTime("2014-01-01 21:55:42");
        DateTime timeNowDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:22:16");

        message.put(TIMESTAMP, secs(timestampDate));
        message.put(FIRST_SWITCHED, secs(firstSwitchDate));
        message.put(BYTES, 999);
        message.put(PKTS, 99);

        List<Map<String, Object>> expected = new ArrayList<>();
        Map<String, Object> msgExpected = new HashMap<>();
        msgExpected.put(TIMESTAMP, secs(formatter.withZoneUTC().parseDateTime("2014-01-01 21:51:00")));
        msgExpected.put(BYTES, 145L);
        msgExpected.put(PKTS, 14L);
        Map<String, Object> msgExpected1 = new HashMap<>();
        msgExpected1.put(TIMESTAMP, secs(formatter.withZoneUTC().parseDateTime("2014-01-01 21:52:00")));
        msgExpected1.put(BYTES, 181L);
        msgExpected1.put(PKTS, 18L);
        Map<String, Object> msgExpected2 = new HashMap<>();
        msgExpected2.put(TIMESTAMP, secs(formatter.withZoneUTC().parseDateTime("2014-01-01 21:53:00")));
        msgExpected2.put(BYTES, 181L);
        msgExpected2.put(PKTS, 18L);
        Map<String, Object> msgExpected3 = new HashMap<>();
        msgExpected3.put(TIMESTAMP, secs(formatter.withZoneUTC().parseDateTime("2014-01-01 21:54:00")));
        msgExpected3.put(BYTES, 181L);
        msgExpected3.put(PKTS, 18L);
        Map<String, Object> msgExpected4 = new HashMap<>();
        msgExpected4.put(TIMESTAMP, secs(formatter.withZoneUTC().parseDateTime("2014-01-01 21:55:00")));
        msgExpected4.put(BYTES, 181L);
        msgExpected4.put(PKTS, 18L);
        Map<String, Object> msgExpected5 = new HashMap<>();
        msgExpected5.put(TIMESTAMP, secs(formatter.withZoneUTC().parseDateTime("2014-01-01 21:55:42")));
        msgExpected5.put(BYTES, 130L);
        msgExpected5.put(PKTS, 13L);
        expected.add(msgExpected);
        expected.add(msgExpected1);
        expected.add(msgExpected2);
        expected.add(msgExpected3);
        expected.add(msgExpected4);
        expected.add(msgExpected5);


        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertEquals(expected, result);
    }

    @Test
    public void acceptsOldMessagesInTheRealtimeWindow() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.withZoneUTC().parseDateTime("2014-01-01 21:50:12");
        DateTime timestampDate = formatter.withZoneUTC().parseDateTime("2014-01-01 21:50:42");
        DateTime timeNowDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:12:16");

        message.put(TIMESTAMP, secs(timestampDate));
        message.put(FIRST_SWITCHED, secs(firstSwitchDate));
        message.put(BYTES, 999);
        message.put(PKTS, 99);

        List<Map<String, Object>> expected = new ArrayList<>();
        Map<String, Object> msgExpected = new HashMap<>();
        msgExpected.put(TIMESTAMP, secs(timestampDate));
        msgExpected.put(BYTES, 999L);
        msgExpected.put(PKTS, 99L);
        expected.add(msgExpected);

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertEquals(expected, result);
    }

    @Test
    public void correctsMessagesInTheFuture() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.withZoneUTC().parseDateTime("2014-01-01 21:55:12");
        DateTime timestampDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:01:42");
        DateTime timeNowDate = formatter.withZoneUTC().parseDateTime("2014-01-01 21:36:16");

        message.put(TIMESTAMP, secs(timestampDate));
        message.put(FIRST_SWITCHED, secs(firstSwitchDate));
        message.put(BYTES, 999);
        message.put(PKTS, 99);

        List<Map<String, Object>> expectedPackets = new ArrayList<>();
        Map<String, Object> expected;
        DateTime pktTime;

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2014-01-01 21:36:16");
        expected.put(TIMESTAMP, secs(pktTime));
        expected.put(BYTES, 999L);
        expected.put(PKTS, 99L);
        expectedPackets.add(expected);

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertEquals(expectedPackets, result);
    }

    @Test
    public void correctsMessagesInTheFutureAndSplits() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.withZoneUTC().parseDateTime("2014-01-01 21:45:12");
        DateTime timestampDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:01:42");
        DateTime timeNowDate = formatter.withZoneUTC().parseDateTime("2014-01-01 21:47:16");

        message.put(TIMESTAMP, secs(timestampDate));
        message.put(FIRST_SWITCHED, secs(firstSwitchDate));
        message.put(BYTES, 999);
        message.put(PKTS, 99);

        List<Map<String, Object>> expectedPackets = new ArrayList<>();
        Map<String, Object> expected;
        DateTime pktTime;

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2014-01-01 21:46:00");
        expected.put(TIMESTAMP, secs(pktTime));
        expected.put(BYTES, 386L);
        expected.put(PKTS, 38L);
        expectedPackets.add(expected);

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2014-01-01 21:47:00");
        expected.put(TIMESTAMP, secs(pktTime));
        expected.put(BYTES, 483L);
        expected.put(PKTS, 47L);
        expectedPackets.add(expected);

        expected = new HashMap<>();
        pktTime = formatter.withZoneUTC().parseDateTime("2014-01-01 21:47:16");
        expected.put(TIMESTAMP, secs(pktTime));
        expected.put(BYTES, 130L);
        expected.put(PKTS, 14L);
        expectedPackets.add(expected);

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertEquals(expectedPackets, result);
    }

    @Test
    public void removesExtraDurationPackets() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:10:12");
        DateTime timestampDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:13:42");
        DateTime timeNowDate = formatter.withZoneUTC().parseDateTime("2014-01-01 22:15:16");

        message.put(TIMESTAMP, secs(timestampDate));
        message.put(FIRST_SWITCHED, secs(firstSwitchDate));
        message.put(DURATION, 94);
        message.put(BYTES, 999);
        message.put(PKTS, 99);

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertEquals(result.get(0).get(DURATION), 94);
        assertNull(result.get(1).get(DURATION));
        assertNull(result.get(2).get(DURATION));
        assertNull(result.get(3).get(DURATION));
    }

    @Test
    public void numberFormatExceptions() {
        Map<String, Object> message = new HashMap<>();

        DateTime timestampDate = formatter.parseDateTime("2014-01-01 22:13:42");
        DateTime timeNowDate = formatter.parseDateTime("2014-01-01 22:15:16");

        message.put(TIMESTAMP, secs(timestampDate));
        message.put(BYTES, 2000000000.0);
        message.put(PKTS, 15625000);

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertTrue(result.isEmpty());
    }

    @Test
    public void eventWithoutBytes() {
        Map<String, Object> message = new HashMap<>();

        DateTime timestampDate = formatter.parseDateTime("2014-01-01 22:13:42");
        DateTime timeNowDate = formatter.parseDateTime("2014-01-01 22:15:16");

        message.put(TIMESTAMP, secs(timestampDate));
        message.put(PKTS, 100);

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertTrue(result.isEmpty());
    }
}