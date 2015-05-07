package net.redborder.samza.enrichments;

import java.util.Map;

public interface IEnrich {
    Map<String, Object> enrich(Map<String, Object> message);
}
