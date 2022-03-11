package ibm.eda.kstreams.lab.infra;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import ibm.eda.kstreams.lab.domain.FinancialMessage;

/**
     * An interface for registering types that can be de/serialized with {@link JSONSerde}.
     */
    @SuppressWarnings("DefaultAnnotationParam") // being explicit for the example
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
    @JsonSubTypes({
                      @JsonSubTypes.Type(value = FinancialMessage.class, name = "fm")
                  })
public interface JSONSerdeCompatible {
    
}
