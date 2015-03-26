package net.redborder.samza.util;

import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockKeyValueStore implements KeyValueStore<String, Map<String, Object>> {
    Map<String, Map<String, Object>> store = new HashMap<>();

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