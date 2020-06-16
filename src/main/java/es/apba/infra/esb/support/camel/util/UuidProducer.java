package es.apba.infra.esb.support.camel.util;

import java.util.UUID;

/**
 * Producer of UUIDs
 * 
 * @author fsaucedo
 */
public class UuidProducer {
    
    /**
     * Produce an UUID
     * 
     * @return UUID string
     */
    public String getUUID() {
        return UUID.randomUUID().toString();
    }
    
}
