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
