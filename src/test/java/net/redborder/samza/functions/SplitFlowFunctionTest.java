package net.redborder.samza.functions;

import junit.framework.TestCase;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
        message.put("timestamp", secs(currentTime));
        message.put("bytes", 999);
        message.put("pkts", 99);

        List<Map<String, Object>> result = SplitFlowFunction.split(message);
        assertEquals(message, result.get(0));
    }

    @Test
    public void withoutTimestamp() {
        Map<String, Object> message = new HashMap<>();
        message.put("bytes", 999);
        message.put("pkts", 99);

        List<Map<String, Object>> result = SplitFlowFunction.split(message);
        message.put("timestamp", secs(currentTime));
        assertEquals(message, result.get(0));
    }

    @Test
    public void lessThanAMinute() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.parseDateTime("2014-01-01 22:10:12");
        DateTime timestampDate = formatter.parseDateTime("2014-01-01 22:10:42");
        DateTime timeNowDate = formatter.parseDateTime("2014-01-01 22:15:16");

        message.put("timestamp", secs(timestampDate));
        message.put("first_switched", secs(firstSwitchDate));
        message.put("bytes", 999);
        message.put("pkts", 99);

        Map<String, Object> expected = new HashMap<>();
        expected.put("timestamp", secs(timestampDate));
        expected.put("bytes", 999L);
        expected.put("pkts", 99L);

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertEquals(expected, result.get(0));
    }

    @Test
    public void higherThanAMinute() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.parseDateTime("2014-01-01 22:10:12");
        DateTime timestampDate = formatter.parseDateTime("2014-01-01 22:13:42");
        DateTime timeNowDate = formatter.parseDateTime("2014-01-01 22:15:16");

        message.put("timestamp", secs(timestampDate));
        message.put("first_switched", secs(firstSwitchDate));
        message.put("bytes", 999);
        message.put("pkts", 99);

        List<Map<String, Object>> expectedPackets = new ArrayList<>();
        Map<String, Object> expected;
        DateTime pktTime;

        expected = new HashMap<>();
        pktTime = formatter.parseDateTime("2014-01-01 22:11:00");
        expected.put("timestamp", secs(pktTime));
        expected.put("bytes", 228L);
        expected.put("pkts", 22L);
        expectedPackets.add(expected);

        expected = new HashMap<>();
        pktTime = formatter.parseDateTime("2014-01-01 22:12:00");
        expected.put("timestamp", secs(pktTime));
        expected.put("bytes", 285L);
        expected.put("pkts", 28L);
        expectedPackets.add(expected);

        expected = new HashMap<>();
        pktTime = formatter.parseDateTime("2014-01-01 22:13:00");
        expected.put("timestamp", secs(pktTime));
        expected.put("bytes", 285L);
        expected.put("pkts", 28L);
        expectedPackets.add(expected);

        expected = new HashMap<>();
        pktTime = formatter.parseDateTime("2014-01-01 22:13:42");
        expected.put("timestamp", secs(pktTime));
        expected.put("bytes", 201L);
        expected.put("pkts", 21L);
        expectedPackets.add(expected);

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertEquals(expectedPackets, result);
    }

    @Test
    public void correctsFirstSwitchedMessages() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.parseDateTime("2014-01-01 21:58:12");
        DateTime timestampDate = formatter.parseDateTime("2014-01-01 22:01:42");
        DateTime timeNowDate = formatter.parseDateTime("2014-01-01 22:22:16");

        message.put("timestamp", secs(timestampDate));
        message.put("first_switched", secs(firstSwitchDate));
        message.put("bytes", 999);
        message.put("pkts", 99);

        List<Map<String, Object>> expectedPackets = new ArrayList<>();
        Map<String, Object> expected;
        DateTime pktTime;

        expected = new HashMap<>();
        pktTime = formatter.parseDateTime("2014-01-01 22:01:00");
        expected.put("timestamp", secs(pktTime));
        expected.put("bytes", 511L);
        expected.put("pkts", 50L);
        expectedPackets.add(expected);

        expected = new HashMap<>();
        pktTime = formatter.parseDateTime("2014-01-01 22:01:42");
        expected.put("timestamp", secs(pktTime));
        expected.put("bytes", 488L);
        expected.put("pkts", 49L);
        expectedPackets.add(expected);

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertEquals(expectedPackets, result);
    }

    @Test
    public void dropOldMessages() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.parseDateTime("2014-01-01 21:50:12");
        DateTime timestampDate = formatter.parseDateTime("2014-01-01 21:55:42");
        DateTime timeNowDate = formatter.parseDateTime("2014-01-01 22:22:16");

        message.put("timestamp", secs(timestampDate));
        message.put("first_switched", secs(firstSwitchDate));
        message.put("bytes", 999);
        message.put("pkts", 99);

        List<Map<String, Object>> expected = new ArrayList<>();

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertEquals(expected, result);
    }

    @Test
    public void acceptsOldMessagesInTheRealtimeWindow() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.parseDateTime("2014-01-01 21:50:12");
        DateTime timestampDate = formatter.parseDateTime("2014-01-01 21:50:42");
        DateTime timeNowDate = formatter.parseDateTime("2014-01-01 22:12:16");

        message.put("timestamp", secs(timestampDate));
        message.put("first_switched", secs(firstSwitchDate));
        message.put("bytes", 999);
        message.put("pkts", 99);

        List<Map<String, Object>> expected = new ArrayList<>();
        Map<String, Object> msgExpected = new HashMap<>();
        msgExpected.put("timestamp", secs(timestampDate));
        msgExpected.put("bytes", 999L);
        msgExpected.put("pkts", 99L);
        expected.add(msgExpected);

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertEquals(expected, result);
    }

    @Test
    public void correctsMessagesInTheFuture() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.parseDateTime("2014-01-01 21:55:12");
        DateTime timestampDate = formatter.parseDateTime("2014-01-01 22:01:42");
        DateTime timeNowDate = formatter.parseDateTime("2014-01-01 21:36:16");

        message.put("timestamp", secs(timestampDate));
        message.put("first_switched", secs(firstSwitchDate));
        message.put("bytes", 999);
        message.put("pkts", 99);

        List<Map<String, Object>> expectedPackets = new ArrayList<>();
        Map<String, Object> expected;
        DateTime pktTime;

        expected = new HashMap<>();
        pktTime = formatter.parseDateTime("2014-01-01 21:36:16");
        expected.put("timestamp", secs(pktTime));
        expected.put("bytes", 999L);
        expected.put("pkts", 99L);
        expectedPackets.add(expected);

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertEquals(expectedPackets, result);
    }

    @Test
    public void correctsMessagesInTheFutureAndSplits() {
        Map<String, Object> message = new HashMap<>();
        DateTime firstSwitchDate = formatter.parseDateTime("2014-01-01 21:45:12");
        DateTime timestampDate = formatter.parseDateTime("2014-01-01 22:01:42");
        DateTime timeNowDate = formatter.parseDateTime("2014-01-01 21:47:16");

        message.put("timestamp", secs(timestampDate));
        message.put("first_switched", secs(firstSwitchDate));
        message.put("bytes", 999);
        message.put("pkts", 99);

        List<Map<String, Object>> expectedPackets = new ArrayList<>();
        Map<String, Object> expected;
        DateTime pktTime;

        expected = new HashMap<>();
        pktTime = formatter.parseDateTime("2014-01-01 21:46:00");
        expected.put("timestamp", secs(pktTime));
        expected.put("bytes", 386L);
        expected.put("pkts", 38L);
        expectedPackets.add(expected);

        expected = new HashMap<>();
        pktTime = formatter.parseDateTime("2014-01-01 21:47:00");
        expected.put("timestamp", secs(pktTime));
        expected.put("bytes", 483L);
        expected.put("pkts", 47L);
        expectedPackets.add(expected);

        expected = new HashMap<>();
        pktTime = formatter.parseDateTime("2014-01-01 21:47:16");
        expected.put("timestamp", secs(pktTime));
        expected.put("bytes", 130L);
        expected.put("pkts", 14L);
        expectedPackets.add(expected);

        List<Map<String, Object>> result = SplitFlowFunction.split(message, timeNowDate);
        assertEquals(expectedPackets, result);
    }
}