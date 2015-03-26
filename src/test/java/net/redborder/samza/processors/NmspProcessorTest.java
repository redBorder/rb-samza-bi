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

import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.MockKeyValueStore;
import org.apache.samza.storage.kv.KeyValueStore;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NmspProcessorTest {
    NmspProcessor nmspProcessor;

    @Mock
    StoreManager storeManager;

    @Before
    public void initTest() {
        KeyValueStore<String, Map<String, Object>> store = new MockKeyValueStore();

        storeManager = mock(StoreManager.class);
        when(storeManager.getStore(NmspProcessor.NMSP_STORE)).thenReturn(store);

        nmspProcessor = new NmspProcessor(storeManager);
    }

    @Test
    public void processReturnsNull() {

    }
}
