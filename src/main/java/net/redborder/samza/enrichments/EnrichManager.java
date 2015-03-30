package net.redborder.samza.enrichments;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 30/3/15 12:26.
 */
public class EnrichManager {

    List<IEnrich> enrichments;

    public EnrichManager() {
        enrichments = new ArrayList<>();
    }

    public void addEnrichment(IEnrich enrich) {
        enrichments.add(enrich);
    }

    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, Object> enrichments = new HashMap<>();
        enrichments.putAll(message);

        for (IEnrich enrich : this.enrichments) {
            enrichments.putAll(enrich.enrich(message));
        }

        return enrichments;
    }
}
