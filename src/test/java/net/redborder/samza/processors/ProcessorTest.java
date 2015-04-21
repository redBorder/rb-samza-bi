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

package net.redborder.samza.processors;

import junit.framework.TestCase;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.MockMessageCollector;
import net.redborder.samza.util.MockTaskContext;
import org.apache.samza.config.Config;
import org.apache.samza.config.ConfigException;
import org.apache.samza.task.TaskContext;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProcessorTest extends TestCase {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    static TaskContext taskContext;

    @BeforeClass
    public static void initTest() {
        taskContext = new MockTaskContext();
    }

    @Test
    public void getProcessorInstantiatesTheCorrectProcessor() {
        Config config = mock(Config.class);
        when(config.get("redborder.processors.rb_flow")).thenReturn("net.redborder.samza.processors.FlowProcessor");
        Processor p = Processor.getProcessor("rb_flow", config, taskContext, null);
        assertEquals("flow", p.getName());
    }

    @Test
    public void getProcessorReturnsDummyWhenClassNotFound() {
        Config config = mock(Config.class);
        when(config.get("redborder.processors.rb_nmsp")).thenReturn("net.redborder.samza.processors.NotFoundProcessor");
        Processor p = Processor.getProcessor("rb_nmsp", config, taskContext, null);
        assertEquals("dummy", p.getName());
    }

    @Test
    public void streamWithoutProcessorThrowsConfigException() {
        Config config = mock(Config.class);
        when(config.get("redborder.processors.rb_nmsp")).thenThrow(new ConfigException("Not found"));
        exception.expect(ConfigException.class);
        Processor p = Processor.getProcessor("rb_nmsp", config, taskContext, null);
    }

    @Test
    public void getProcessorWithoutEnrichmentsWorks() {
        Config config = mock(Config.class);
        when(config.get("redborder.enrichments.streams.rb_flow")).thenThrow(new ConfigException("Not found"));
        when(config.get("redborder.processors.rb_flow")).thenReturn("net.redborder.samza.processors.FlowProcessor");

        TaskContext context = mock(TaskContext.class);
        StoreManager storeManager = new StoreManager(config, context);
        Processor p = Processor.getProcessor("rb_flow", config, taskContext, storeManager);

        Map<String, Object> message = new HashMap<>();
        message.put(CLIENT_MAC, "AA:AA:AA:AA:AA:AA");
        message.put(BYTES, 43L);
        message.put(PKTS, 3L);
        message.put(TIMESTAMP, Long.valueOf(1429088471L));

        MockMessageCollector collector = new MockMessageCollector();
        p.process(message, collector);

        Map<String, Object> result = collector.getResult().get(0);
        message.put(DURATION, 0L);
        assertEquals(message, result);
    }
}


