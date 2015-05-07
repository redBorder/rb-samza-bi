package net.redborder.samza.enrichments;

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;

@RunWith(MockitoJUnitRunner.class)
public class GeoIpEnrichTest extends TestCase {

    @Test
    public void enrichesWitGeoIp() {
        Map<String, Object> result = new HashMap<>();

        GeoIpEnrich.ASN_DB_PATH = "/this_path_does_not_exist";
        GeoIpEnrich.ASN_V6_DB_PATH = "/this_path_does_not_exist";
        GeoIpEnrich.CITY_DB_PATH = "/this_path_does_not_exist";
        GeoIpEnrich.CITY_V6_DB_PATH = "/this_path_does_not_exist";

        GeoIpEnrich geoIpEnrich = new GeoIpEnrich();
        Map<String, Object> message = new HashMap<>();

        message.put(SRC_IP, "86.121.44.1");
        message.put(DST_IP, "2a02:26f0:8:183::90");

        result.putAll(message);

        Map<String, Object> enrichMessage = geoIpEnrich.enrich(message);
        assertEquals(result, enrichMessage);

        result.clear();

        GeoIpEnrich.ASN_DB_PATH = ClassLoader.getSystemResource("asn.dat").getFile();
        GeoIpEnrich.ASN_V6_DB_PATH = ClassLoader.getSystemResource("asnv6.dat").getFile();
        GeoIpEnrich.CITY_DB_PATH = ClassLoader.getSystemResource("city.dat").getFile();
        GeoIpEnrich.CITY_V6_DB_PATH = ClassLoader.getSystemResource("cityv6.dat").getFile();

        geoIpEnrich = new GeoIpEnrich();

        message.clear();

        message.put(SRC_IP, "86.121.44.1");
        message.put(DST_IP, "2a02:26f0:8:183::90");

        result.putAll(message);
        result.put(SRC_COUNTRY_CODE, "RO");
        result.put(DST_COUNTRY_CODE, "EU");
        result.put(SRC_AS_NAME, "RCS & RDS SA");
        result.put(DST_AS_NAME, "Akamai Technologies European AS");

        enrichMessage = geoIpEnrich.enrich(message);
        assertEquals(result, enrichMessage);
    }
}
