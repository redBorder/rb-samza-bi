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

import org.apache.samza.container.TaskName;
import org.apache.samza.metrics.MetricsRegistry;
import org.apache.samza.system.SystemStreamPartition;
import org.apache.samza.task.TaskContext;

import java.util.Set;

public class MockTaskContext implements TaskContext {
    @Override
    public MetricsRegistry getMetricsRegistry() {
        return new MockMetricsRegistry();
    }

    @Override
    public Set<SystemStreamPartition> getSystemStreamPartitions() {
        return null;
    }

    @Override
    public Object getStore(String s) {
        return new MockKeyValueStore();
    }

    @Override
    public TaskName getTaskName() {
        return null;
    }

    @Override
    public void setStartingOffset(SystemStreamPartition systemStreamPartition, String s) {
        // Do nothing
    }
}
