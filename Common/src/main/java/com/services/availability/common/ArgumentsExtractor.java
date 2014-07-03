package com.services.availability.common;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Roman Reva
 * @version 1.0
 * @since 2014-06-24 16:23
 */
public class ArgumentsExtractor {
    public static Map<String, String> extract(String [] requestedArgs, String [] appArgs) {
        Map<String, String> values = new HashMap<String, String>();
        for (String appArg: appArgs) {
            for (String requestedArg: requestedArgs) {
                String argString = new StringBuilder().append("-").append(requestedArg).append("=").toString();
                if (appArg.contains(argString)) {
                    String value = appArg.replace(argString, "");
                    values.put(requestedArg, value);
                }
            }
        }
        return values;
    }
}
