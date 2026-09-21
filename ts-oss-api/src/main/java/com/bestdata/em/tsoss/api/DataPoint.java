package com.bestdata.em.tsoss.api;

import lombok.Getter;

/**
 * 时序数据点：一个时间戳 + 一个测值。
 */
@Getter
public final class DataPoint {

    /** 时间戳，epoch 毫秒。 */
    private final long tm;

    /** 测值，float 型。 */
    private final float value;

    /** 传感器 ID。可选。 */
    private String sid;

    /** 设备ID */
    private String deviceId;

    /** 数据点 ID。默认为时间戳。 */
    private String id;

    public DataPoint(String sensorId, long timestamp, float value) {
        this.sid = sensorId;
        this.tm = timestamp;
        this.value = value;
        this.id = String.valueOf(timestamp);
    }

    public DataPoint(String sensorId, String deviceId, long timestamp, float value) {
        this(sensorId, timestamp, value);
        this.deviceId = deviceId;
    }

    public DataPoint(long timestamp, float value) {
        this.tm = timestamp;
        this.value = value;
    }

    @Override
    public String toString() {
        return "DataPoint{id=" + id + ", sid=" + sid + ", deviceId=" + deviceId + ", " +
                "tm=" + tm + ", value=" + value + '}';
    }
}
