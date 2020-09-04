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
     * @param exchange Camel exception
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
            boolean result;
            boolean lastExceptionWasConnectException = getLastExceptionWasConnectExceptionProperty(exchange);
            int consumerRedeliveryCounter = getConsumerRedeliveryCounterProperty(exchange);

            boolean redeliveryCausedByConnectException = isCausedByConnectException(ex);

            if (redeliveryCausedByConnectException) {
                result = consumerRedeliveryCounter < connectExceptionMaximumRedeliveries;
            } else {
                if (lastExceptionWasConnectException) {
                    consumerRedeliveryCounter = 0;

                    logConsumerRedeliveryCounterReset(exchange, consumerRedeliveryCounter);
                }

                result = consumerRedeliveryCounter < otherExceptionMaximumRedeliveries;
            }

            logRedelivery(exchange, consumerRedeliveryCounter);
            logExceptionDetails(exchange, redeliveryCausedByConnectException);

            setConsumerRedeliveryCounterProperty(exchange, ++consumerRedeliveryCounter);
            setLastExceptionWasConnectExceptionProperty(exchange, redeliveryCausedByConnectException);

            return result;
        }
    }

    /**
     * Look for a Connect Exception in the stack trace
     *
     * @param ex Exception
     * @return If there is a Connect Exception in the stack trace
     */
    private boolean isCausedByConnectException(Exception ex) {
        if (ex instanceof ConnectException) {
            return true;
        } else {
            for (Throwable t = ex.getCause(); t != null; t = t.getCause()) {
                if (t instanceof ConnectException) {
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Log a redelivery
     *
     * @param exchange Camel exchange
     * @param redeliveryCounter Redelivery counter
     */
    void logRedelivery(Exchange exchange, int redeliveryCounter) {
        logRedeliveryWarning(exchange, "Retry", redeliveryCounter);
    }

    /**
     * Log a reset on a consumer redelivery counter
     *
     * @param exchange Camel exchange
     * @param redeliveryCounter Redelivery counter
     */
    void logConsumerRedeliveryCounterReset(Exchange exchange, int consumerRedeliveryCounter) {
        logRedeliveryWarning(exchange, "Consumer redelivery counter reset", consumerRedeliveryCounter);
    }

    /**
     * Log a redelivery warning message
     *
     * @param exchange Camel exchange
     * @param operation Operation
     * @param redeliveryCounter Redelivery counter
     */
    void logRedeliveryWarning(Exchange exchange, String operation, int redeliveryCounter) {
        LOG.warn("{} - {} - {} | {} | PROCESS | apbaTransactionId: {} | counter: {}",
                exchange.getContext().getName(),
                exchange.getFromRouteId(),
                exchange.getExchangeId(),
                operation,
                exchange.getIn().getHeader("apbaTransactionId"),
                redeliveryCounter);
    }
    
    /**
     * Log the exception details
     * 
     * @param exchange Camel exchange
     * @param redeliveryCausedByConnectException If true log a standard message
     */
    void logExceptionDetails(Exchange exchange, boolean redeliveryCausedByConnectException) {
        LOG.warn("{} - {} - {} | {} | PROCESS | apbaTransactionId: {} | name: {} | message: {}",
                exchange.getContext().getName(),
                exchange.getFromRouteId(),
                exchange.getExchangeId(),
                "Exception details",
                exchange.getIn().getHeader("apbaTransactionId"),
                redeliveryCausedByConnectException ? ConnectException.class.getName() : exchange.getException().getClass().getName(),
                redeliveryCausedByConnectException ? "Exception on connecting to the target" : exchange.getException().getMessage());
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
     * Getter for lastExceptionWasConnectException property
     *
     * @param Camel exchange
     * @return LastExceptionWasConnectException exchange property
     */
    private boolean getLastExceptionWasConnectExceptionProperty(Exchange exchange) {
        Boolean lastExceptionWasConnectException = exchange.getProperty("lastExceptionWasConnectException", Boolean.class);

        return lastExceptionWasConnectException == null ? false : lastExceptionWasConnectException;
    }

    /**
     * Setter for lastExceptionWasConnectException property
     *
     * @param exchange Camel exchange
     * @param lastExceptionWasConnectException LastExceptionWasConnectException
     * exchange property
     */
    private void setLastExceptionWasConnectExceptionProperty(Exchange exchange, boolean lastExceptionWasConnectException) {
        exchange.setProperty("lastExceptionWasConnectException", lastExceptionWasConnectException);
    }

}
