package net.redborder.samza.processors;

import net.redborder.samza.enrichments.EnrichManager;
import net.redborder.samza.store.StoreManager;
import net.redborder.samza.util.constants.Constants;

import static net.redborder.samza.util.constants.Dimension.*;

import org.apache.samza.config.Config;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.system.SystemStream;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.TaskContext;

import java.util.HashMap;
import java.util.Map;

public class LocationLogicProcessor extends Processor<Map<String, Object>> {
    private static final SystemStream OUTPUT_STREAM = new SystemStream("kafka", Constants.ENRICHMENT_LOC_OUTPUT_TOPIC);
    private KeyValueStore<String, Map<String, Object>> storeLogic;
    public final static String LOCATION_STORE_LOGIC = "location-logic";

    public LocationLogicProcessor(StoreManager storeManager, EnrichManager enrichManager, Config config, TaskContext context) {
        super(storeManager, enrichManager, config, context);
        this.storeManager = storeManager;
        this.enrichManager = enrichManager;
        storeLogic = storeManager.getStore(LOCATION_STORE_LOGIC);
    }

    @Override
    public void process(Map<String, Object> message, MessageCollector collector) {
        String type = (String) message.get(TYPE);
        if (type.equals("mse10") || type.equals("mse") || type.equals("nmsp-info") || type.equals("nmsp-measure")) {
            Map<String, Object> toDruid = new HashMap<>();
            Map<String, Object> toCache = new HashMap<>();

            String client_mac = (String) message.get(CLIENT_MAC);
            String newFloor = (String) message.get(FLOOR);
            String newBuilding = (String) message.get(BUILDING);
            String newCampus = (String) message.get(CAMPUS);
            String newZone = (String) message.get(ZONE);
            String wirelessStation = (String) message.get(WIRELESS_STATION);

            Map<String, Object> locationCache = storeLogic.get(client_mac);

            if (newFloor == null)
                newFloor = "unknown";

            if (newBuilding == null)
                newBuilding = "unknown";

            if (newCampus == null)
                newCampus = "unknown";

            if (newZone == null)
                newZone = "unknown";

            if (wirelessStation == null)
                wirelessStation = "unknown";

            if (locationCache != null) {
                String oldFloor = (String) locationCache.get(FLOOR);
                String oldBuilding = (String) locationCache.get(BUILDING);
                String oldCampus = (String) locationCache.get(CAMPUS);
                String oldwirelessStation = (String) locationCache.get(WIRELESS_STATION);
                String oldZone = (String) locationCache.get(ZONE);

                if (oldFloor != null)
                    if (!oldFloor.equals(newFloor)) {
                        toDruid.put(FLOOR_OLD, oldFloor);
                        toDruid.put(FLOOR_NEW, newFloor);
                    } else {
                        toDruid.put(FLOOR, newFloor);
                    }

                if (oldZone != null)
                    if (!oldZone.equals(newZone)) {
                        toDruid.put(ZONE_OLD, oldZone);
                        toDruid.put(ZONE_NEW, newZone);
                    } else {
                        toDruid.put(ZONE, newZone);
                    }

                if (oldwirelessStation != null)
                    if (!oldwirelessStation.equals(wirelessStation)) {
                        toDruid.put(WIRELESS_STATION_OLD, oldwirelessStation);
                        toDruid.put(WIRELESS_STATION_NEW, wirelessStation);
                    } else {
                        toDruid.put(WIRELESS_STATION, wirelessStation);
                    }

                if (oldBuilding != null)
                    if (!oldBuilding.equals(newBuilding)) {
                        toDruid.put(BUILDING_OLD, oldBuilding);
                        toDruid.put(BUILDING_NEW, newBuilding);
                    } else {
                        toDruid.put(BUILDING, newBuilding);
                    }

                if (oldCampus != null)
                    if (!oldCampus.equals(newCampus)) {
                        toDruid.put(CAMPUS_OLD, oldCampus);
                        toDruid.put(CAMPUS_NEW, newCampus);
                    } else {
                        toDruid.put(CAMPUS, newCampus);
                    }

            } else {
                toDruid.put(FLOOR_NEW, newFloor);
                toDruid.put(CAMPUS_NEW, newCampus);
                toDruid.put(BUILDING_NEW, newBuilding);
                toDruid.put(ZONE_NEW, newZone);
                toDruid.put(WIRELESS_STATION_NEW, wirelessStation);
            }


            toDruid.put(CLIENT_MAC, client_mac);
            toDruid.put(TIMESTAMP, message.get(TIMESTAMP));

            if (message.containsKey(SENSOR_NAME))
                toDruid.put(SENSOR_NAME, message.get(SENSOR_NAME));

            if (message.containsKey(TYPE))
                toDruid.put(TYPE, message.get(TYPE));

            if (message.containsKey(NAMESPACE))
                toDruid.put(NAMESPACE, message.get(NAMESPACE));

            if (message.containsKey(NAMESPACE_UUID))
                toDruid.put(NAMESPACE_UUID, message.get(NAMESPACE_UUID));

            if (message.containsKey(DEPLOYMENT))
                toDruid.put(DEPLOYMENT, message.get(DEPLOYMENT));

            if (message.containsKey(DEPLOYMENT_UUID))
                toDruid.put(DEPLOYMENT_UUID, message.get(DEPLOYMENT_UUID));

            if (message.containsKey(MARKET))
                toDruid.put(MARKET, message.get(MARKET));

            if (message.containsKey(MARKET_UUID))
                toDruid.put(MARKET_UUID, message.get(MARKET_UUID));

            if (message.containsKey(ORGANIZATION))
                toDruid.put(ORGANIZATION, message.get(ORGANIZATION));

            if (message.containsKey(ORGANIZATION_UUID))
                toDruid.put(ORGANIZATION_UUID, message.get(ORGANIZATION_UUID));

            toCache.put(FLOOR, newFloor);
            toCache.put(CAMPUS, newCampus);
            toCache.put(BUILDING, newBuilding);
            toCache.put(ZONE, newZone);
            toCache.put(WIRELESS_STATION, wirelessStation);

            storeLogic.put(client_mac, toCache);

            collector.send(new OutgoingMessageEnvelope(OUTPUT_STREAM, null, toDruid));
        }
    }

    @Override
    public String getName() {
        return "location-logic";
    }
}
