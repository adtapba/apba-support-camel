package es.apba.infra.esb.support.camel.processor;

import java.net.ConnectException;
import org.apache.camel.Exchange;
import org.apache.camel.Predicate;
import org.apache.camel.processor.RedeliveryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Custom redelivery policy for APBA consumers
 * 
 * @author fsaucedo
 */
public class ConsumerRedeliveryPolicy extends RedeliveryPolicy {
    
    private static final long serialVersionUID = 2373209455431813365L;
    
    private final Logger LOG = LoggerFactory.getLogger(ConsumerRedeliveryPolicy.class);
    
    private final int connectExceptionMaximumRedeliveries;    
    private final int otherExceptionMaximumRedeliveries;    
    
    /**
     * Parametrized constructor
     * 
     * @param connectExceptionMaximumRedeliveries Maximum redeliveries for java.net.ConnectException
     * @param otherExceptionMaximumRedeliveries Maximum redeliveries for other exceptions
     * @param onExceptionRedeliveryDelay Redelivery delay
     */
    public ConsumerRedeliveryPolicy(int connectExceptionMaximumRedeliveries, 
            int otherExceptionMaximumRedeliveries, 
            long onExceptionRedeliveryDelay) {
        super();
        
        this.connectExceptionMaximumRedeliveries = connectExceptionMaximumRedeliveries;
        this.otherExceptionMaximumRedeliveries = otherExceptionMaximumRedeliveries;
        
        setMaximumRedeliveries(connectExceptionMaximumRedeliveries);
        setRedeliveryDelay(onExceptionRedeliveryDelay);
    }
        
    /**
     * Custom implementation for APBA consumers with diferent number of redeliveries 
     * depending on the exception
     * 
     * @param exchange  Camel exception
     * @param redeliveryCounter Not used for this implementation
     * @param retryWhile Not used for this implementation
     * @return Decision on the delivery of one message
     */
    @Override
    public boolean shouldRedeliver(Exchange exchange, int redeliveryCounter, Predicate retryWhile) {
        Exception ex = exchange.getException();       
        
        if (ex == null) {
            throw new UnsupportedOperationException("This redelivery policy must be used within an exception control clause");
        } else {
            Exception lastException = getLastExceptionProperty(exchange);
            int consumerRedeliveryCounter = getConsumerRedeliveryCounterProperty(exchange);
            boolean result;
            
            if (ex instanceof ConnectException) {
                result = consumerRedeliveryCounter < connectExceptionMaximumRedeliveries;                              
            } else {
                if (lastException != null && lastException instanceof ConnectException) {
                    consumerRedeliveryCounter = 0;  
                    
                    logConsumerRedeliveryCounterReset(exchange, consumerRedeliveryCounter);
                }
                
                result = consumerRedeliveryCounter < otherExceptionMaximumRedeliveries;
            }
            
            logRedelivery(exchange, consumerRedeliveryCounter);
            
            setConsumerRedeliveryCounterProperty(exchange, ++consumerRedeliveryCounter);
            setLastExceptionProperty(exchange, ex);
            
            return result;
        }
    }   
    
    /**
     * Log a redelivery
     * 
     * @param exchange Camel exchange
     * @param redeliveryCounter Redelivery counter
     */
    void logRedelivery(Exchange exchange, int redeliveryCounter) {
        logWarning(exchange, "Retry", redeliveryCounter);
    }

    /**
     * Log a reset on a consumer redelivery counter
     * 
     * @param exchange Camel exchange
     * @param redeliveryCounter Redelivery counter
     */
    void logConsumerRedeliveryCounterReset(Exchange exchange, int consumerRedeliveryCounter) {
        logWarning(exchange, "Consumer redelivery counter reset", consumerRedeliveryCounter);
    }
    
    void logWarning(Exchange exchange, String operation, int redeliveryCounter) {
        LOG.warn("{} - {} - {} | {} | PROCESS | apbaTransactionId: {} | counter: {}", 
                exchange.getContext().getName(),
                exchange.getFromRouteId(),
                exchange.getExchangeId(),
                operation,
                exchange.getIn().getHeader("apbaTransactionId"),
                redeliveryCounter);
    }

    /**
     * Getter for consumer redelivery counter property
     * 
     * @param exchange Camel exchange
     * @return Consumer redelivery counter value
     */
    int getConsumerRedeliveryCounterProperty(Exchange exchange) {
        Integer consumerRedeliveryCounter = exchange.getProperty("consumerRedeliveryCounter", Integer.class);
        
        return consumerRedeliveryCounter == null ? 0 : consumerRedeliveryCounter;                        
    }
    
    /**
     * Setter for consumer redelivery counter property
     * 
     * @param exchange Camel exchange
     * @param consumerRedeliveryCounter Consumer redelivery counter value
     */
    void setConsumerRedeliveryCounterProperty(Exchange exchange, int consumerRedeliveryCounter) {
        exchange.setProperty("consumerRedeliveryCounter", consumerRedeliveryCounter);
    }

    /**
     * Getter for last exception property
     * 
     * @param exchange Camel exchange
     * @return Last exception value
     */
    Exception getLastExceptionProperty(Exchange exchange) {
        return exchange.getProperty("lastException", Exception.class);
    }

    /**
     * Setter for last exception property
     * 
     * @param exchange Camel exchange
     * @param lastException Last exception value 
     */
    void setLastExceptionProperty(Exchange exchange, Exception lastException) {
        exchange.setProperty("lastException", lastException);
    }
        
}
