package dev.thebooleanguy.threadpool;

public class PoolStateHolder {
    private PoolState state;

    public PoolStateHolder(PoolState state) {
        this.state = state;
    }

    public PoolState getState() {
        return state;
    }

    public void setState(PoolState state) {
        this.state = state;
    }

}
