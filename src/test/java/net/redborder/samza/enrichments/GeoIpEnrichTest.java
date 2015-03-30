package net.redborder.samza.enrichments;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import static net.redborder.samza.util.constants.Dimension.*;

/**
 * Date: 30/3/15 17:17.
 */
public class GeoIpEnrichTest extends TestCase {


    @Test
    public void enrichesWitGeoIp() {
        GeoIpEnrich.ASN_DB_PATH = ClassLoader.getSystemResource("src/test/resources/asn.dat").getFile();
        GeoIpEnrich.ASN_V6_DB_PATH = ClassLoader.getSystemResource("src/test/resources/asnv6.dat").getFile();
        GeoIpEnrich.CITY_DB_PATH = ClassLoader.getSystemResource("src/test/resources/city.dat").getFile();
        GeoIpEnrich.ASN_V6_DB_PATH = ClassLoader.getSystemResource("src/test/resources/cityv6.dat").getFile();

        GeoIpEnrich geoIpEnrich = new GeoIpEnrich();

        Map<String, Object> message = new HashMap<>();

        message.put(SRC_IP, "86.121.44.1");
        message.put(DST_IP, "78.2.11.2");

        Map<String, Object> enrichMessage = geoIpEnrich.enrich(message);
        System.out.println(enrichMessage);

    }
}
