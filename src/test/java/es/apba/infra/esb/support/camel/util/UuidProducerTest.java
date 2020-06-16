package es.apba.infra.esb.support.camel.util;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;

/**
 * Tester for UuidProducer
 * 
 * @author fsaucedo
 */
public class UuidProducerTest {
    
    UuidProducer instance = new UuidProducer();
    
    @Test
    public void whenGetUuid_thenProduceUuid() {
        String uuid = instance.getUUID();
        
        assertThat(uuid.matches("[0-9a-fA-F]{8}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{12}"), is(true));
    }
    
}
