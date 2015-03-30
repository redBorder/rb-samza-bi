package net.redborder.samza.enrichments;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import static net.redborder.samza.util.constants.Dimension.*;

public class MacVendorEnrich implements IEnrich {
    private static final Logger log = LoggerFactory.getLogger(MacVendorEnrich.class);

    public static String ouiFilePath = "/opt/rb/etc/objects/oui-vendors";
    public Map<String, String> ouiMap;

    public MacVendorEnrich() {
        InputStream in = null;
        ouiMap = new HashMap<>();

        try {
            in = new FileInputStream(ouiFilePath);
        } catch (FileNotFoundException ex) {
            log.error("The MacVendor file couldn't be found", ex);
        }

        if (in != null) {
            InputStreamReader isr = new InputStreamReader(in);
            BufferedReader br = new BufferedReader(isr);

            try {
                String line = br.readLine();

                while (line != null) {
                    String[] tokens = line.split("\\|");
                    ouiMap.put(tokens[0].substring(2, 8), tokens[1]);
                    line = br.readLine();
                }
            } catch (IOException ex) {
                log.error("Couldn't process MacVendor file", ex);
            }
        }
    }

    private String buildOui(Object object) {
        String mac = object.toString();
        mac = mac.trim().replace("-", "").replace(":", "");
        return mac.substring(0, 6).toUpperCase();
    }

    @Override
    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, Object> vendorMap = new HashMap<>();
        vendorMap.putAll(message);

        String clientMac = (String) message.get(CLIENT_MAC);

        if (clientMac != null) {
            String oui = buildOui(clientMac);
            if (ouiMap.get(oui) != null)
                vendorMap.put(CLIENT_MAC_VENDOR, ouiMap.get(oui));
        }

        return vendorMap;
    }
}
