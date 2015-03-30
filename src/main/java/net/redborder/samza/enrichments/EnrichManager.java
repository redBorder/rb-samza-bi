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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Date: 30/3/15 12:26.
 */
public class EnrichManager {

    List<IEnrich> enrichments;

    public EnrichManager() {
        enrichments = new ArrayList<>();
    }

    public void addEnrichment(IEnrich enrich) {
        enrichments.add(enrich);
    }

    public Map<String, Object> enrich(Map<String, Object> message) {
        Map<String, Object> enrichments = new HashMap<>();
        enrichments.putAll(message);

        for (IEnrich enrich : this.enrichments) {
            enrichments.putAll(enrich.enrich(message));
        }

        return enrichments;
    }
}
