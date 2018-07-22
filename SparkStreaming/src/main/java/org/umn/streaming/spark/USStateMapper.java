package org.umn.streaming.spark;

import java.util.HashMap;

public class USStateMapper {

    private static HashMap<String, String> state_mapping;
    private static String UNDEFINED = "UND";

    static {
        state_mapping = new HashMap<>();
        state_mapping.put("alabama", "AL");
        state_mapping.put("alaska", "AK");
        state_mapping.put("arizona", "AZ");
        state_mapping.put("arkansas", "AR");
        state_mapping.put("california", "CA");
        state_mapping.put("colorado", "CO");
        state_mapping.put("connecticut", "CT");
        state_mapping.put("delaware", "DE");
        state_mapping.put("florida", "FL");
        state_mapping.put("georgia", "GA");
        state_mapping.put("hawaii", "HI");
        state_mapping.put("idaho", "ID");
        state_mapping.put("illinois", "IL");
        state_mapping.put("indiana", "IN");
        state_mapping.put("iowa", "IA");
        state_mapping.put("kansas", "KS");
        state_mapping.put("kentucky", "KY");
        state_mapping.put("louisiana", "LA");
        state_mapping.put("maine", "ME");
        state_mapping.put("maryland", "MD");
        state_mapping.put("massachusetts", "MA");
        state_mapping.put("michigan", "MI");
        state_mapping.put("minnesota", "MN");
        state_mapping.put("mississippi", "MS");
        state_mapping.put("missouri", "MO");
        state_mapping.put("montana", "MT");
        state_mapping.put("nebraska", "NE");
        state_mapping.put("nevada", "NV");
        state_mapping.put("new hampshire", "NH");
        state_mapping.put("new jersey", "NJ");
        state_mapping.put("new mexico", "NM");
        state_mapping.put("new york", "NY");
        state_mapping.put("north carolina", "NC");
        state_mapping.put("north dakota", "ND");
        state_mapping.put("ohio", "OH");
        state_mapping.put("oklahoma", "OK");
        state_mapping.put("oregon", "OR");
        state_mapping.put("pennsylvania", "PA");
        state_mapping.put("rhode island", "RI");
        state_mapping.put("south carolina", "SC");
        state_mapping.put("south dakota", "SD");
        state_mapping.put("tennessee", "TN");
        state_mapping.put("texas", "TX");
        state_mapping.put("utah", "UT");
        state_mapping.put("vermont", "VT");
        state_mapping.put("virginia", "VA");
        state_mapping.put("washington", "WA");
        state_mapping.put("west virginia", "WV");
        state_mapping.put("wisconsin", "WI");
        state_mapping.put("wyoming", "WY");
    }

    public static String getStateShortCode(String place_name) {
        String[] split_names = place_name.split(",");
        if(split_names.length == 2) {
            if(split_names[1].trim().length() == 2) {
                return split_names[1];
            } else if (split_names[1].trim().length() == 3 && split_names[1].trim().equalsIgnoreCase("USA")) {
                return state_mapping.getOrDefault(split_names[0].trim().toLowerCase(), UNDEFINED);
            }
        } else if(split_names.length == 1) {
            return state_mapping.getOrDefault(split_names[0].trim().toLowerCase(), UNDEFINED);
        }
        return UNDEFINED;
    }
}
