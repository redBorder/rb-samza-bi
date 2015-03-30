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
import org.apache.samza.config.Config;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ProcessorTest extends TestCase {
    @Test
    public void getProcessorInstantiatesTheCorrectProcessor() {
        Config config = mock(Config.class);
        when(config.get("redborder.processors.rb_flow")).thenReturn("net.redborder.samza.processors.FlowProcessor");
        Processor p = Processor.getProcessor("rb_flow", config, null);
        assertEquals("flow", p.getName());
    }

    @Test
    public void getProcessorReturnsDummyWhenClassNotFound() {
        Config config = mock(Config.class);
        when(config.get("redborder.processors.rb_nmsp")).thenReturn("net.redborder.samza.processors.NotFoundProcessor");
        Processor p = Processor.getProcessor("rb_nmsp", config, null);
        assertEquals("dummy", p.getName());
    }
}

