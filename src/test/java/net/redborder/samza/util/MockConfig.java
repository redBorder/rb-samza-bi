package net.redborder.samza.util;


import org.apache.samza.config.Config;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class MockConfig extends Config {
    Map<String, String> hash = new HashMap<>();

    @Override
    public Config sanitize() {
        return null;
    }

    @Override
    public int size() {
        return 0;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        return false;
    }

    @Override
    public boolean containsValue(Object value) {
        return false;
    }

    @Override
    public String get(Object key) {
        return null;
    }

    @Override
    public Set<String> keySet() {
        return null;
    }

    @Override
    public Collection<String> values() {
        return null;
    }

    @Override
    public Set<Entry<String, String>> entrySet() {
        return null;
    }

    @Override
    public String get(String key, String defaultString) {
        String value;

        if (hash.containsKey(key)) {
            value = hash.get(key);
        } else {
            value = defaultString;
        }

        return value;
    }
}
