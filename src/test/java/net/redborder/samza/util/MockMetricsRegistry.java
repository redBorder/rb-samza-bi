package net.redborder.samza.util;

import org.apache.samza.metrics.Counter;
import org.apache.samza.metrics.Gauge;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.metrics.Timer;

public class MockMetricsRegistry implements MetricsRegistry {
    @Override
    public Counter newCounter(String s, String s1) {
        return new Counter(s1);
    }

    @Override
    public Counter newCounter(String s, Counter counter) {
        return counter;
    }

    @Override
    public <T> Gauge<T> newGauge(String s, String s1, T t) {
        return new Gauge<>(s1, t);
    }

    @Override
    public <T> Gauge<T> newGauge(String s, Gauge<T> gauge) {
        return gauge;
    }

    @Override
    public Timer newTimer(String s, String s1) {
        return new Timer(s1);
    }

    @Override
    public Timer newTimer(String s, Timer timer) {
        return timer;
    }
}
