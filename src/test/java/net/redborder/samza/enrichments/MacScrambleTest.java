package net.redborder.samza.enrichments;

import junit.framework.TestCase;
import net.redborder.samza.util.MacScramble;
import net.redborder.samza.util.constants.Dimension;
import org.bouncycastle.util.encoders.Hex;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.security.GeneralSecurityException;

@RunWith(MockitoJUnitRunner.class)
public class MacScrambleTest extends TestCase {
    private final static char[] HEX_CHARS = "0123456789abcdef".toCharArray();


    @Test
    public void macScramble() throws GeneralSecurityException {
        MacScramble macScramble = new MacScramble(Hex.decode("6A285A12B390A55E021EB983B20D5F4E"), null);
        assertEquals("ec:19:1a:2d:07:e7", toMac(macScramble.scrambleMac(Hex.decode("b418d16e8101".replace(":", ""))), ":"));
        assertEquals("cb:36:a8:1a:d9:2c", toMac(macScramble.scrambleMac(Hex.decode("b418d16e8102".replace(":", ""))), ":"));
        assertEquals("50:07:f2:72:c4:7b", toMac(macScramble.scrambleMac(Hex.decode("b418d16e8103".replace(":", ""))), ":"));
        assertEquals("92:62:10:43:52:5b", toMac(macScramble.scrambleMac(Hex.decode("b418d16e8104".replace(":", ""))), ":"));

    }

    public static String toMac(final byte[] val, final String sep) {
        final StringBuilder sb = new StringBuilder(32);
        for (int a = 0; a < val.length; a++) {
            if (sb.length() > 0) {
                sb.append(sep);
            }
            sb.append(HEX_CHARS[(val[a] >> 4) & 0x0F]);
            sb.append(HEX_CHARS[val[a] & 0x0F]);
        }

        return sb.toString();
    }

}
