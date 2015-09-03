package net.redborder.samza.enrichments;

import junit.framework.TestCase;
import net.redborder.samza.util.MacScramble;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.security.GeneralSecurityException;

@RunWith(MockitoJUnitRunner.class)
public class MacScrambleTest extends TestCase {


    @Test
    public void macScramble() throws GeneralSecurityException {
        MacScramble macScramble = new MacScramble("6A285A12B390A55E021EB983B20D5F4E".getBytes(), null);

        byte scrambleMac[] = macScramble.scrambleMac(hexStringToByteArray("b4:18:d1:6e:86:a5".replace(":", "")));

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < scrambleMac.length; i++) {
            sb.append(String.format("%02X%s", scrambleMac[i], (i < scrambleMac.length - 1) ? ":" : ""));
        }
        String mac = sb.toString().toLowerCase();

        assertEquals("b8:81:ee:4a:47:56", mac);
    }

    public byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];

        for (int i = 0; i < len/2; i += 1) {
            String element = s.substring(i*2, i*2+2);
            data[i] = (byte) Integer.parseInt(element, 16);
        }

        return data;
    }

}
