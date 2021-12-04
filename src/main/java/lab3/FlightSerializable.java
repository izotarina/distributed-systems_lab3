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

    public void setDelay(double delay) {
        this.delay = delay;
    }

    public boolean getIsCancelled() {
        return isCancelled;
    }

    public void setCancelled(boolean cancelled) {
        isCancelled = cancelled;
    }
}
