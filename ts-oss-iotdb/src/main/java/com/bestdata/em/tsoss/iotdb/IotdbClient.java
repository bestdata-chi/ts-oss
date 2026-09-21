package com.bestdata.em.tsoss.iotdb;

import com.bestdata.em.tsoss.api.DataPoint;
import com.bestdata.em.tsoss.api.PageResult;
import com.bestdata.em.tsoss.api.TimeSeriesClient;
import org.apache.iotdb.session.pool.SessionPool;
import java.util.List;
import java.util.Map;

/**
 * IoTDB 连接句柄，包装 {@link SessionPool}。
 */
public class IotdbClient implements TimeSeriesClient, AutoCloseable {

    private final SessionPool sessionPool;
    private final String database;
    private volatile boolean closed = false;

    IotdbClient(SessionPool sessionPool, String database) {
        this.sessionPool = sessionPool;
        this.database = database;
    }

    @Override
    public boolean isConnected() {
        return sessionPool != null && !closed;
    }

    @Override
    public List<DataPoint> query(String deviceId, String sensorId, long startTime, long endTime) {
        return null;
    }

    @Override
    public List<DataPoint> query(List<DataPoint> dataPoints, long startTime, long endTime) {
        return null;
    }

    @Override
    public List<Map<String, Object>> query(String sql) {
        return null;
    }

    @Override
    public void voidQuery(String sql) {

    }

    @Override
    public PageResult<DataPoint> queryPage(String deviceId, String sensorId, long startTime, long endTime, int page, int limit) {
        return null;
    }

    @Override
    public List<DataPoint> queryRealTime(String deviceId, String sensorId) {
        return null;
    }

    @Override
    public void write(DataPoint dataPoint) {
    }

    @Override
    public void batchWrite(List<DataPoint> dataPoints) {

    }

    @Override
    public void delete(String deviceId, String sensorId, long startTime, long endTime) {

    }

    @Override
    public void batchDelete(List<Map<String, Object>> deleteList) {

    }

    @Override
    public void close() {
        closed = true;
        sessionPool.close();
    }

}
