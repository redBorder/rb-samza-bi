package net.redborder.samza.enrichments;

import net.redborder.samza.util.MacScramble;
import net.redborder.samza.util.PostgresqlManager;
import net.redborder.samza.util.constants.Dimension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.GeneralSecurityException;
import java.util.Map;

public class MacScramblingEnrich implements IEnrich {
    private static final Logger log = LoggerFactory.getLogger(MacScramblingEnrich.class);

    @Override
    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, MacScramble> scrambles = PostgresqlManager.getScrambles();

        String mac = (String) message.get(Dimension.CLIENT_MAC);
        String spUUID = (String) message.get(Dimension.SERVICE_PROVIDER_UUID);

        try {
            byte scrambleMac [];

            MacScramble scramble = scrambles.get(spUUID);

            log.debug("SPuuid: {}  Scramble: {}", spUUID, scramble);
            if(scramble != null) {
                scrambleMac = scramble.scrambleMac(hexStringToByteArray(mac.replace(":", "")));

                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < scrambleMac.length; i++) {
                    sb.append(String.format("%02X%s", scrambleMac[i], (i < scrambleMac.length - 1) ? ":" : ""));
                }
                mac = sb.toString().toLowerCase();

                message.put(Dimension.CLIENT_MAC, mac);
            }
        } catch (GeneralSecurityException e) {
            e.printStackTrace();
        }


        return message;
    }

    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];

        for (int i = 0; i < len/2; i += 1) {
            String element = s.substring(i*2, i*2+2);
            data[i] = (byte) Integer.parseInt(element, 16);
        }

        return data;
    }
}
