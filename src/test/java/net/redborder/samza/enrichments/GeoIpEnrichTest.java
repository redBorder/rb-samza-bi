/*
 * Copyright (c) 2015 ENEO Tecnologia S.L.
 * This file is part of redBorder.
 * redBorder is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * redBorder is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with redBorder. If not, see <http://www.gnu.org/licenses/>.
 */

package net.redborder.samza.enrichments;

import junit.framework.TestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import static net.redborder.samza.util.constants.Dimension.*;

@RunWith(MockitoJUnitRunner.class)
public class GeoIpEnrichTest extends TestCase {

    @Test
    public void enrichesWitGeoIp() {

        Map<String, Object> result = new HashMap<>();

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
