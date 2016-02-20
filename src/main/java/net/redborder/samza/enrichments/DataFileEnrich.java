package net.redborder.samza.enrichments;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.redborder.samza.util.PostgresqlManager;
import net.redborder.samza.util.constants.Dimension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class DataFileEnrich implements IEnrich {
    private static final Logger log = LoggerFactory.getLogger(DataFileEnrich.class);
    private static final String file = "/tmp/radius_client.json";
    Map<String, Map<String, Object>> data;

    public DataFileEnrich() {
        try {
            data = (Map<String, Map<String, Object>>) new ObjectMapper().readValue(new File(file), Map.class);
        } catch (IOException e) {
            log.error("Error reading [" + file + "]", e);
            data = new HashMap<>();
        }
    }

    @Override
    public Map<String, Object> enrich(Map<String, Object> message) {
        String clientId = (String) message.get(Dimension.CLIENT_ID);

        if (clientId != null) {
            Map<String, Object> enrichData = data.get(clientId);

            if (enrichData != null) {
                message.putAll(enrichData);
            }
        }

        return message;
    }

    @Override
    public void setPostgresqlManager(PostgresqlManager postgresqlManager) {

    }
}
