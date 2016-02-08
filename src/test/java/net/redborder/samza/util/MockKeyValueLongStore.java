package net.redborder.samza.util;

import org.apache.samza.storage.kv.Entry;
import org.apache.samza.storage.kv.KeyValueIterator;
import org.apache.samza.storage.kv.KeyValueStore;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockKeyValueLongStore implements KeyValueStore<String, Long> {
    Map<String, Long> store = new HashMap<>();

    public boolean isEmpty() {
        return store.isEmpty();
    }

    @Override
    public Long get(String s) {
        return store.get(s);
    }

    @Override
    public void put(String s, Long o) {
        store.put(s, o);
    }

    @Override
    public void putAll(List<Entry<String, Long>> list) {
    }

    @Override
    public void delete(String s) {
        store.remove(s);
    }

    @Override
    public Map<String, Long> getAll(List<String> list) {
        return null;
    }

    @Override
    public void deleteAll(List<String> list) {

    }

    @Override
    public KeyValueIterator<String, Long> range(String s, String k1) {
        return null;
    }

    @Override
    public KeyValueIterator<String, Long> all() {
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