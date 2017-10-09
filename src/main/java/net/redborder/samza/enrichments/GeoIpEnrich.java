package net.redborder.samza.enrichments;

import com.maxmind.geoip.Location;
import com.maxmind.geoip.LookupService;
import net.redborder.samza.util.PostgresqlManager;
import org.apache.samza.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static net.redborder.samza.util.constants.Dimension.*;

public class GeoIpEnrich implements IEnrich {
    private static final Logger log = LoggerFactory.getLogger(GeoIpEnrich.class);

    /**
     * Path to city data base.
     */
    public static String CITY_DB_PATH = "/opt/rb/share/GeoIP/city.dat";
    /**
     * Path to city v6 data base.
     */
    public static String CITY_V6_DB_PATH = "/opt/rb/share/GeoIP/cityv6.dat";
    /**
     * Path to asn data base.
     */
    public static String ASN_DB_PATH = "/opt/rb/share/GeoIP/asn.dat";
    /**
     * Path to asn v6 data base.
     */
    public static String ASN_V6_DB_PATH = "/opt/rb/share/GeoIP/asnv6.dat";
    /**
     * Pattern to to make the comparison with ips v4.
     */
    public static Pattern VALID_IPV4_PATTERN = null;
    /**
     * Pattern to to make the comparison with ips v6.
     */
    public static Pattern VALID_IPV6_PATTERN = null;
    /**
     * Regular expresion to make the comparison with ipv4 format.
     */
    private static final String ipv4Pattern = "(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])";
    /**
     * Regular expresion to make the comparison with ipv6 format.
     */
    private static final String ipv6Pattern = "([0-9a-f]{1,4}:){7}([0-9a-f]){1,4}";

    /**
     * Reference on memory cache to city data base.
     */
    LookupService city;
    /**
     * Reference on memory cache to city v6 data base.
     */
    LookupService city6;
    /**
     * Reference on memory cache to asn data base.
     */
    LookupService asn;
    /**
     * Reference on memory cache to asn v6 data base.
     */
    LookupService asn6;

    public GeoIpEnrich() {
        try {
            VALID_IPV4_PATTERN = Pattern.compile(ipv4Pattern, Pattern.CASE_INSENSITIVE);
            VALID_IPV6_PATTERN = Pattern.compile(ipv6Pattern, Pattern.CASE_INSENSITIVE);
        } catch (PatternSyntaxException e) {
            log.error("Unable to compile IP check patterns");
        }
    }

    @Override
    public void init(Config config) {
        try {

            city = new LookupService(config.get("redborder.geoip.city.path", CITY_DB_PATH), LookupService.GEOIP_MEMORY_CACHE);
            city6 = new LookupService(config.get("redborder.geoip.cityv6.path", CITY_V6_DB_PATH), LookupService.GEOIP_MEMORY_CACHE);
            asn = new LookupService(config.get("redborder.geoip.asn.path", ASN_DB_PATH), LookupService.GEOIP_MEMORY_CACHE);
            asn6 = new LookupService(config.get("redborder.geoip.asnv6.path", ASN_V6_DB_PATH), LookupService.GEOIP_MEMORY_CACHE);
        } catch (IOException | NullPointerException ex) {
            log.error(ex.toString(), ex);
        }
    }

    /**
     * <p>Query if there is a country code for a given IP.</p>
     *
     * @param ip This is the address to query the data base.
     * @return The country code, example: US, ES, FR.
     */

    private String getCountryCode(String ip) {
        Matcher match = VALID_IPV4_PATTERN.matcher(ip);
        String countryCode = null;
        Location location = null;

        if (match.matches()) {
            if (city != null)
                location = city.getLocation(ip);
        } else {
            if (city6 != null)
                location = city6.getLocationV6(ip);
        }

        if (location != null) {
            countryCode = location.countryCode;
        }

        return countryCode;
    }

    /**
     * <p>Query if there is a asn for a given IP.</p>
     *
     * @param ip This is the address to query the data base.
     * @return The asn name.
     */
    private String getAsnName(String ip) {
        Matcher match = VALID_IPV4_PATTERN.matcher(ip);
        String asnName = null;
        String asnInfo = null;

        if (match.matches()) {
            if (asn != null)
                asnInfo = asn.getOrg(ip);
        } else {
            if (asn6 != null)
                asnInfo = asn6.getOrgV6(ip);
        }

        if (asnInfo != null) {
            String[] asn = asnInfo.split(" ", 2);

            if (asn.length > 1) {
                if (asn[1] != null) asnName = asn[1];
            } else {
                if (asn[0] != null) asnName = asn[0];
            }
        }
        return asnName;
    }

    @Override
    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, Object> geoIPMap = new HashMap<>();
        geoIPMap.putAll(message);

        String src = (String) message.get(LAN_IP);
        String dst = (String) message.get(WAN_IP);

        if (src != null) {
            String country_code = null;
            String asn_name = null;

            if (VALID_IPV4_PATTERN != null) {
                country_code = getCountryCode(src);
                asn_name = getAsnName(src);
            }

            if (country_code != null) {
                geoIPMap.put(LAN_IP_COUNTRY_CODE, country_code);
                geoIPMap.put(IP_COUNTRY_CODE, country_code);
            }

            if (asn_name != null) geoIPMap.put(LAN_IP_AS_NAME, asn_name);
        }

        if (dst != null) {
            String country_code = null;
            String asn_name = null;

            if (VALID_IPV4_PATTERN != null) {
                country_code = getCountryCode(dst);
                asn_name = getAsnName(dst);
            }

            if (country_code != null) {
                geoIPMap.put(WAN_IP_COUNTRY_CODE, country_code);
                geoIPMap.put(IP_COUNTRY_CODE, country_code);
            }

            if (asn_name != null) geoIPMap.put(WAN_IP_AS_NAME, asn_name);
        }

        return geoIPMap;
    }

    @Override
    public void setPostgresqlManager(PostgresqlManager postgresqlManager) {
        //Nothing
    }
}
