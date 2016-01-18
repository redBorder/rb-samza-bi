package net.redborder.samza.enrichments;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.redborder.samza.util.PostgresqlManager;
import net.redborder.samza.util.constants.Dimension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DarkListEnrich implements IEnrich {
    private static final Logger log = LoggerFactory.getLogger(DarkListEnrich.class);

    final String IP_FILE = "/tmp/darklist.json";
    Map<String, Map<String, Object>> ipCache = new HashMap<>();

    public DarkListEnrich() {
        ObjectMapper mapper = new ObjectMapper();

        try {
            List<Map<String, Object>> ipDatas = mapper.readValue(new File(IP_FILE), List.class);

            for (Map<String, Object> ipData : ipDatas) {
                String ip = (String) ipData.get("ip");
                Map<String, Object> data = (Map<String, Object>) ipData.get("enrich_with");

                ipCache.put(ip, data);
            }
        } catch (IOException e) {
            log.error("Error initiating darklist cache from file. ", e);
        }

        log.info("Created DarklistCache: {}", ipCache.keySet().toString());
    }

    @Override
    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, Object> enrichment = new HashMap<>();
        enrichment.putAll(message);

        String src = (String) message.get("src");
        String dst = (String) message.get("dst");

        if (src != null && dst != null) {
            Map<String, Object> srcData = ipCache.get(src);
            Map<String, Object> dstData = ipCache.get(dst);

            if (srcData != null && dstData != null) {
                Integer srcScore = (Integer) srcData.get("darklist_score");
                Integer dstScore = (Integer) dstData.get("darklist_score");

                if (srcScore > dstScore) {
                    enrichment.putAll(srcData);
                } else {
                    enrichment.putAll(dstData);
                }

                enrichment.put("darklist_direction", "both");
            } else if (srcData != null) {
                enrichment.putAll(srcData);
                enrichment.put("darklist_direction", "source");
            } else if (dstData != null) {
                enrichment.putAll(dstData);
                enrichment.put("darklist_direction", "destination");
            } else {
                enrichment.put(Dimension.DARKLIST_DIRECTION, "clean");
                enrichment.put(Dimension.DARKLIST_CATEGORY, "clean");
            }

        } else if (src != null) {
            Map<String, Object> srcData = ipCache.get(src);
            if (srcData != null) {
                enrichment.putAll(srcData);
                enrichment.put("darklist_direction", "source");
            } else {
                enrichment.put(Dimension.DARKLIST_DIRECTION, "clean");
                enrichment.put(Dimension.DARKLIST_CATEGORY, "clean");
            }
        } else if (dst != null) {
            Map<String, Object> dstData = ipCache.get(dst);
            if (dstData != null) {
                enrichment.putAll(dstData);
                enrichment.put("darklist_direction", "destination");
            } else {
                enrichment.put(Dimension.DARKLIST_DIRECTION, "clean");
                enrichment.put(Dimension.DARKLIST_CATEGORY, "clean");
            }
        } else {
            enrichment.put(Dimension.DARKLIST_DIRECTION, "clean");
            enrichment.put(Dimension.DARKLIST_CATEGORY, "clean");
        }

        return enrichment;
    }

    @Override
    public void setPostgresqlManager(PostgresqlManager postgresqlManager) {
        //Nothing
    }
}
