package lab3;

import java.io.Serializable;

public class FlightSerializable implements Serializable {
    private double delay;
    private boolean isCancelled;

    public FlightSerializable(double delay, boolean isCancelled) {
        this.delay = delay;
        this.isCancelled = isCancelled;
    }

    public double getDelay() {
        return delay;
    }

    public boolean getIsCancelled() {
        return isCancelled;
    }
}
