package net.redborder.samza.enrichments;

import java.util.ArrayList;
import java.util.List;

/**
 * Date: 30/3/15 12:26.
 */
public class EnrichManager {

    List<IEnrich> enrichments;

    public EnrichManager(){
        enrichments = new ArrayList<>();
    }

    public void addEnrichment(IEnrich enrich){
        enrichments.add(enrich);
    }

}
