package net.redborder.samza.enrichments;

import net.redborder.samza.util.MacScramble;
import net.redborder.samza.util.PostgresqlManager;
import net.redborder.samza.util.constants.Dimension;

import java.security.GeneralSecurityException;
import java.util.Map;

public class MacScrambling implements IEnrich {

    @Override
    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, MacScramble> scrambles = PostgresqlManager.getScrambles();

        String mac = (String) message.get(Dimension.CLIENT_MAC);
        String spUUID = (String) message.get(Dimension.SERVICE_PROVIDER_UUID);

        try {
            byte scrambleMac [];

            MacScramble scramble = scrambles.get(spUUID);

            if(scramble != null) {
                scrambleMac = scramble.scrambleMac(mac.getBytes());

                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < scrambleMac.length; i++) {
                    sb.append(String.format("%02X%s", scrambleMac[i], (i < scrambleMac.length - 1) ? ":" : ""));
                }
                mac = sb.toString();

                message.put(Dimension.CLIENT_MAC, mac);
            }
        } catch (GeneralSecurityException e) {
            e.printStackTrace();
        }


        return message;
    }
}
