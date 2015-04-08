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
