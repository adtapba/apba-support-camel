package es.apba.infra.esb.support.camel.processor;

import java.net.ConnectException;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tester for ConsumerRedeliveryPolicy
 *
 * @author fsaucedo
 */
public class ConsumerRedeliveryPolicyTest {
    
    private static final int REGULAR_MAXIMUM_REDELIVERIES_HAS_NO_EFFECT = 0;
    private static final Exception OTHER_EXCEPTION = mock(Exception.class);
    private static final Exception CONNECT_EXCEPTION = mock(ConnectException.class);
        
    @Test(expected = UnsupportedOperationException.class)
    public void when_ExchangeWithoutException_then_ThrowUnsupportedOperationException() {
        ConsumerRedeliveryPolicy instance = 
                getInstance_TenMaximumRedeliveriesForConnectException_TwoMaximumRedeliveriesForOtherException(); 
        Exchange exchange = getExchange();
        
        instance.shouldRedeliver(exchange, REGULAR_MAXIMUM_REDELIVERIES_HAS_NO_EFFECT, null);
    }
    
    @Test
    public void when_ExchangeContainsConnectException_and_IsFirstExcecution_then_ReturnTrue() {
        ConsumerRedeliveryPolicy instance = 
                getInstance_TenMaximumRedeliveriesForConnectException_TwoMaximumRedeliveriesForOtherException(); 
        Exchange exchange = getExchangeWithAConnectException();
        when(exchange.getProperty("consumerRedeliveryCounter", Integer.class)).thenReturn(null);  
        when(exchange.getProperty("lastException", Exception.class)).thenReturn(null);
        
        assertThat(instance.shouldRedeliver(exchange, REGULAR_MAXIMUM_REDELIVERIES_HAS_NO_EFFECT, null)
                , is(true)); 
        verify(exchange, times(1)).setProperty("consumerRedeliveryCounter", 1);  
        verify(exchange, times(1)).setProperty("lastException", CONNECT_EXCEPTION);
    }
    
    @Test
    public void when_ExchangeContainsOtherException_and_IsFirstExcecution_then_ReturnTrue() {
        ConsumerRedeliveryPolicy instance = 
                getInstance_TenMaximumRedeliveriesForConnectException_TwoMaximumRedeliveriesForOtherException(); 
        Exchange exchange = getExchangeWithOtherException();
        when(exchange.getProperty("consumerRedeliveryCounter", Integer.class)).thenReturn(null);  
        when(exchange.getProperty("lastException", Exception.class)).thenReturn(null);
        
        assertThat(instance.shouldRedeliver(exchange, REGULAR_MAXIMUM_REDELIVERIES_HAS_NO_EFFECT, null)
                , is(true)); 
        verify(exchange, times(1)).setProperty("consumerRedeliveryCounter", 1);  
        verify(exchange, times(1)).setProperty("lastException", OTHER_EXCEPTION);
    }
    
    @Test
    public void when_ExchangeContainsConnectException_and_RedeliveryCounterLowerThanConnectExceptionMaximumRedeliveries_then_ReturnTrue() {
        ConsumerRedeliveryPolicy instance = 
                getInstance_TenMaximumRedeliveriesForConnectException_TwoMaximumRedeliveriesForOtherException(); 
        Exchange exchange = getExchangeWithAConnectException();
        when(exchange.getProperty("consumerRedeliveryCounter", Integer.class)).thenReturn(9);  
        when(exchange.getProperty("lastException", Exception.class)).thenReturn(CONNECT_EXCEPTION);
        
        assertThat(instance.shouldRedeliver(exchange, REGULAR_MAXIMUM_REDELIVERIES_HAS_NO_EFFECT, null)
                , is(true));
        verify(exchange, times(1)).setProperty("consumerRedeliveryCounter", 10);  
        verify(exchange, times(1)).setProperty("lastException", CONNECT_EXCEPTION);
    }
    
    @Test
    public void when_ExchangeContainsConnectException_and_RedeliveryCounterEqualsToConnectExceptionMaximumRedeliveries_then_ReturnFalse() {
        ConsumerRedeliveryPolicy instance = 
                getInstance_TenMaximumRedeliveriesForConnectException_TwoMaximumRedeliveriesForOtherException();
        Exchange exchange = getExchangeWithAConnectException();
        when(exchange.getProperty("consumerRedeliveryCounter", Integer.class)).thenReturn(10);  
        when(exchange.getProperty("lastException", Exception.class)).thenReturn(new ConnectException());
        
        assertThat(instance.shouldRedeliver(exchange, REGULAR_MAXIMUM_REDELIVERIES_HAS_NO_EFFECT, null)
                , is(false));
    }
    
    
    @Test
    public void when_ExchangeContainsOtherException_and_RedeliveryCounterLowerThanOtherExceptionMaximumRedeliveries_then_ReturnTrue() {
        ConsumerRedeliveryPolicy instance = 
                getInstance_TenMaximumRedeliveriesForConnectException_TwoMaximumRedeliveriesForOtherException(); 
        Exchange exchange = getExchangeWithOtherException();
        when(exchange.getProperty("consumerRedeliveryCounter", Integer.class)).thenReturn(1);  
        when(exchange.getProperty("lastException", Exception.class)).thenReturn(OTHER_EXCEPTION);
        
        assertThat(instance.shouldRedeliver(exchange, REGULAR_MAXIMUM_REDELIVERIES_HAS_NO_EFFECT, null)
                , is(true));
        verify(exchange, times(1)).setProperty("consumerRedeliveryCounter", 2);  
        verify(exchange, times(1)).setProperty("lastException", OTHER_EXCEPTION);
    }
    
    @Test
    public void when_ExchangeContainsOtherException_and_RedeliveryCounterEqualsToOtherExceptionMaximumRedeliveries_then_ReturnFalse() {
        ConsumerRedeliveryPolicy instance = 
                getInstance_TenMaximumRedeliveriesForConnectException_TwoMaximumRedeliveriesForOtherException();   
        Exchange exchange = getExchangeWithOtherException();
        when(exchange.getProperty("consumerRedeliveryCounter", Integer.class)).thenReturn(2);  
        when(exchange.getProperty("lastException", Exception.class)).thenReturn(OTHER_EXCEPTION);
        
        assertThat(instance.shouldRedeliver(exchange, REGULAR_MAXIMUM_REDELIVERIES_HAS_NO_EFFECT, null)
                , is(false));
    }
    
    @Test
    public void when_ExchangeContainsOtherException_and_lastExceptionWasConnectException_NoMattersCurrentRedeliveryCounter_then_ReturnTrue() {
        ConsumerRedeliveryPolicy instance = 
                getInstance_TenMaximumRedeliveriesForConnectException_TwoMaximumRedeliveriesForOtherException();  
        Exchange exchange = getExchangeWithOtherException();
        when(exchange.getProperty("consumerRedeliveryCounter", Integer.class)).thenReturn(10);     
        when(exchange.getProperty("lastException", Exception.class)).thenReturn(new ConnectException());
        
        assertThat(instance.shouldRedeliver(exchange, REGULAR_MAXIMUM_REDELIVERIES_HAS_NO_EFFECT, null)
                , is(true));  
        verify(exchange, times(1)).setProperty("consumerRedeliveryCounter", 1);  
        verify(exchange, times(1)).setProperty("lastException", OTHER_EXCEPTION);
    }
    
    @Test
    public void when_ExchangeContainsOtherException_and_lastExceptionWasOtherException_and_RedeliveryCounterLowerThanOtherExceptionMaximumRedeliveries_then_ReturnTrue() {
        ConsumerRedeliveryPolicy instance = 
                getInstance_TenMaximumRedeliveriesForConnectException_TwoMaximumRedeliveriesForOtherException();  
        Exchange exchange = getExchangeWithOtherException();
        when(exchange.getProperty("consumerRedeliveryCounter", Integer.class)).thenReturn(1);     
        when(exchange.getProperty("lastException", Exception.class)).thenReturn(OTHER_EXCEPTION);
        
        assertThat(instance.shouldRedeliver(exchange, REGULAR_MAXIMUM_REDELIVERIES_HAS_NO_EFFECT, null)
                , is(true));        
        verify(exchange, times(1)).setProperty("consumerRedeliveryCounter", 2);  
        verify(exchange, times(1)).setProperty("lastException", OTHER_EXCEPTION);
    }
    
    @Test
    public void when_ExchangeContainsOtherException_and_lastExceptionWasOtherException_and_RedeliveryCounterEqualsToOtherExceptionMaximumRedeliveries_then_ReturnFalse() {
        ConsumerRedeliveryPolicy instance = 
                getInstance_TenMaximumRedeliveriesForConnectException_TwoMaximumRedeliveriesForOtherException();   
        Exchange exchange = getExchangeWithOtherException();
        when(exchange.getProperty("consumerRedeliveryCounter", Integer.class)).thenReturn(2);     
        when(exchange.getProperty("lastException", Exception.class)).thenReturn(OTHER_EXCEPTION);
        
        assertThat(instance.shouldRedeliver(exchange, REGULAR_MAXIMUM_REDELIVERIES_HAS_NO_EFFECT, null)
                , is(false));        
    }
    
    @Test
    public void when_ExchangeContainsConnectException_and_lastExceptionWasOtherException_and_RedeliveryCounterEqualsToOtherExceptionMaximumRedeliveries_then_ReturnTrue() {
        ConsumerRedeliveryPolicy instance = 
                getInstance_TenMaximumRedeliveriesForConnectException_TwoMaximumRedeliveriesForOtherException();    
        Exchange exchange = getExchangeWithAConnectException();
        when(exchange.getProperty("consumerRedeliveryCounter", Integer.class)).thenReturn(2);     
        when(exchange.getProperty("lastException", Exception.class)).thenReturn(OTHER_EXCEPTION);
        
        assertThat(instance.shouldRedeliver(exchange, REGULAR_MAXIMUM_REDELIVERIES_HAS_NO_EFFECT, null)
                , is(true));        
        verify(exchange, times(1)).setProperty("consumerRedeliveryCounter", 3);  
        verify(exchange, times(1)).setProperty("lastException", CONNECT_EXCEPTION);
    }
    
    ConsumerRedeliveryPolicy getInstance_TenMaximumRedeliveriesForConnectException_TwoMaximumRedeliveriesForOtherException() {
        return new ConsumerRedeliveryPolicy(10, 2, 30000L);
    }
    
    Exchange getExchangeWithAConnectException() {
        Exchange exchange = getExchange();
        when(exchange.getException()).thenReturn(CONNECT_EXCEPTION);
        
        return exchange;
    }
    
    Exchange getExchangeWithOtherException() {
        Exchange exchange = getExchange();
        when(exchange.getException()).thenReturn(OTHER_EXCEPTION);
        
        return exchange;
    }
    
    Exchange getExchange() {
        Message message = mock(Message.class);
        when(message.getHeader("apbaTransactionId")).thenReturn("Test");
        
        CamelContext camelContext = mock(CamelContext.class);                
        
        Exchange exchange = mock(Exchange.class);
        when(exchange.getContext()).thenReturn(camelContext);
        when(exchange.getIn()).thenReturn(message);
        
        return exchange;
    }
}
