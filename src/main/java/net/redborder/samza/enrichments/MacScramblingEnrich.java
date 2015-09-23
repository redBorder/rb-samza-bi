package net.redborder.samza.enrichments;

import net.redborder.samza.util.MacScramble;
import net.redborder.samza.util.PostgresqlManager;
import net.redborder.samza.util.constants.Dimension;
import org.bouncycastle.util.encoders.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.GeneralSecurityException;
import java.util.Map;

public class MacScramblingEnrich implements IEnrich {
    private static final Logger log = LoggerFactory.getLogger(MacScramblingEnrich.class);
    private final static char[] HEX_CHARS = "0123456789abcdef".toCharArray();

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
                scrambleMac = scramble.scrambleMac(Hex.decode(mac.replace(":", "")));
                message.put(Dimension.CLIENT_MAC, toMac(scrambleMac, ":"));
            }
        } catch (GeneralSecurityException e) {
            e.printStackTrace();
        }


        return message;
    }

    public static String toMac(final byte[] val, final String sep) {
        final StringBuilder sb = new StringBuilder(32);
        for (int a=0;a<val.length;a++) {
            if (sb.length() > 0) {
                sb.append(sep);
            }
            sb.append(HEX_CHARS[(val[a] >> 4) & 0x0F]);
            sb.append(HEX_CHARS[val[a] & 0x0F]);
        }

        return sb.toString();
    }
}
