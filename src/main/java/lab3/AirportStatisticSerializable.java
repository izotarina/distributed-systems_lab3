package lab3;

import java.io.Serializable;

public class AirportStatisticSerializable implements Serializable {
    private final double maxDelay;
    private final double delayedFlights;
    private final double cancelledFlights;
    private final int countFlights;

    public AirportStatisticSerializable(double maxDelay, double delayedFlights, double cancelledFlights, int countFlights) {
        this.maxDelay = maxDelay;
        this.delayedFlights = delayedFlights;
        this.cancelledFlights = cancelledFlights;
        this.countFlights = countFlights;
    }

    public double getCancelledFlights() {
        return cancelledFlights;
    }

    public double getDelayedFlights() {
        return delayedFlights;
    }

    public int getCountFlights() {
        return countFlights;
    }

    public double getMaxDelay() {
        return maxDelay;
    }
}
