package net.redborder.samza.enrichments;

import net.redborder.samza.util.MacScramble;
import net.redborder.samza.util.PostgresqlManager;
import net.redborder.samza.util.constants.Dimension;

import java.security.GeneralSecurityException;
import java.util.Map;

public class MacScrambling implements IEnrich {
    Map<String, MacScramble> scrambles;

    public MacScrambling(){
        for(Map.Entry<String, String> e : PostgresqlManager.getSPSalt().entrySet()){
            scrambles.put(e.getKey(), new MacScramble(e.getValue().getBytes()));
        }
    }

    @Override
    public Map<String, Object> enrich(Map<String, Object> message) {
        String mac = (String) message.get(Dimension.CLIENT_MAC);
        String spUUID = (String) message.get(Dimension.SERVICE_PROVIDER_UUID);
        byte scrambleMac [];

        try {
            scrambleMac = scrambles.get(spUUID).scrambleMac(mac.getBytes());

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < scrambleMac.length; i++) {
                sb.append(String.format("%02X%s", scrambleMac[i], (i < scrambleMac.length - 1) ? ":" : ""));
            }
            mac = sb.toString();

        } catch (GeneralSecurityException e) {
            e.printStackTrace();
        }

        message.put(Dimension.CLIENT_MAC, mac);

        return message;
    }
}
