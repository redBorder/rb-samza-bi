package net.redborder.samza.enrichments;

import java.util.Map;

/**
 * Date: 30/3/15 12:21.
 */
public interface IEnrich {
    Map<String, Object> enrich(Map<String, Object> message);
}
