package net.redborder.samza.enrichments;

import net.redborder.samza.util.PostgresqlManager;

import java.util.Map;

public interface IEnrich {
    Map<String, Object> enrich(Map<String, Object> message);
    void setPostgresqlManager(PostgresqlManager postgresqlManager);
}
