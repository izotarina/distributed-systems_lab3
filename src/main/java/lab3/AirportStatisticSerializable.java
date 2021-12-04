package lab3;

import java.io.Serializable;

public class AirportStatisticSerializable implements Serializable {
    private double maxDelay;
    private double cancelledAndDelayedFlightsPart;

    public AirportStatisticSerializable(double maxDelay, double cancelledAndDelayedFlightsPart) {
        this.maxDelay = maxDelay;
        this.cancelledAndDelayedFlightsPart = cancelledAndDelayedFlightsPart;
    }

    public double getCancelledAndDelayedFlightsPart() {
        return cancelledAndDelayedFlightsPart;
    }

    public double getMaxDelay() {
        return maxDelay;
    }
}
