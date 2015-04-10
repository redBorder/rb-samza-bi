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

import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockKeyValueStore implements KeyValueStore<String, Map<String, Object>> {
    Map<String, Map<String, Object>> store = new HashMap<>();

    public boolean isEmpty() {
        return store.isEmpty();
    }

    @Override
    public Map<String, Object> get(String s) {
        return store.get(s);
    }

    @Override
    public void put(String s, Map<String, Object> o) {
        store.put(s, o);
    }

    @Override
    public void putAll(List<Entry<String, Map<String, Object>>> list) {
    }

    @Override
    public void delete(String s) {
        store.remove(s);
    }

    @Override
    public KeyValueIterator<String, Map<String, Object>> range(String s, String k1) {
        return null;
    }

    @Override
    public KeyValueIterator<String, Map<String, Object>> all() {
        return null;
    }

    @Override
    public void close() {
        store.clear();
        store = null;
    }

    @Override
    public void flush() {
        store.clear();
    }
}