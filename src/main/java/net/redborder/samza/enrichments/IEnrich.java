package net.redborder.samza.enrichments;

import net.redborder.samza.util.PostgresqlManager;
import org.apache.samza.config.Config;

import java.util.Map;

public interface IEnrich {
    void init(Config config);
    Map<String, Object> enrich(Map<String, Object> message);
    void setPostgresqlManager(PostgresqlManager postgresqlManager);
}
