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

import org.apache.samza.task.MessageCollector;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Map;

public class DummyProcessor extends Processor {
    private static final Logger log = LoggerFactory.getLogger(DummyProcessor.class);

    @Override
    public String getName() {
        return "dummy";
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        log.warn("The dummy process method was called!");
    }
}
