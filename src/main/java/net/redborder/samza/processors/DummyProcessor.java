package net.redborder.samza.processors;

import org.apache.samza.task.MessageCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class DummyProcessor extends Processor<Map<String, Object>> {
    private static final Logger log = LoggerFactory.getLogger(DummyProcessor.class);

    public DummyProcessor() {
        super(null, null, null, null);
    }

    @Override
    public String getName() {
        return "dummy";
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        log.warn("The dummy process method was called!");
    }
}
