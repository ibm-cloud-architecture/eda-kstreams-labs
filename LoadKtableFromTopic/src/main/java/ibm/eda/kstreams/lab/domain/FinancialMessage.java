package ibm.eda.kstreams.lab.domain;

import ibm.eda.kstreams.lab.infra.JSONSerdeCompatible;
import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class FinancialMessage implements JSONSerdeCompatible {

    public String userId;
    public String stockSymbol;
    public String exchangeId;
    public int quantity;
    public double stockPrice;
    public double totalCost;
    public int institutionId;
    public int countryId;
    public boolean technicalValidation;

    public FinancialMessage() {

    }

    public FinancialMessage(String userId, String stockSymbol, String exchangeId,
                            int quantity, double stockPrice, double totalCost,
                            int institutionId, int countryId, boolean technicalValidation) {

        this.userId = userId;
        this.stockSymbol = stockSymbol;
        this.exchangeId = exchangeId;
        this.quantity = quantity;
        this.stockPrice = stockPrice;
        this.totalCost = totalCost;
        this.institutionId = institutionId;
        this.countryId = countryId;
        this.technicalValidation = technicalValidation;
    }
}
