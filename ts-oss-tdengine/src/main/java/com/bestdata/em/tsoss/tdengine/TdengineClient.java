package com.bestdata.em.tsoss.tdengine;

import com.bestdata.em.tsoss.api.DataPoint;
import com.bestdata.em.tsoss.api.PageResult;
import com.bestdata.em.tsoss.api.TimeSeriesClient;
import com.bestdata.em.tsoss.core.CommonUtils;
import com.bestdata.em.tsoss.core.ConfigUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * TDengine 连接句柄，基于连接池（HikariCP）按次借还 JDBC {@link Connection}。
 *
 * <p>每次操作从池中借用连接、用完即还，因此多线程并发读写天然安全；断线检测与重建由连接池
 * 在借出时完成，业务方无需关注连接状态。</p>
 */
public class TdengineClient implements TimeSeriesClient, AutoCloseable {

    private final DataSource dataSource;
    private final String superTableName;

    TdengineClient(DataSource dataSource, String superTableName) {
        this.dataSource = dataSource;
        this.superTableName = superTableName;
    }

    @Override
    public boolean isConnected() {
        try (Connection connection = dataSource.getConnection();
             Statement st = connection.createStatement();
             ResultSet rs = st.executeQuery("SELECT SERVER_VERSION()")) {
            return rs.next();
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public List<DataPoint> query(String deviceId, String sensorId, long startTime, long endTime) {
        CommonUtils.requireNotBlank(deviceId, "deviceId");
        if (startTime > endTime) {
            throw new IllegalArgumentException("startTime 不能大于 endTime");
        }
        // 先使用超级表查询，如果发现后期查询效率慢，改变查询方式，直接查询子表名
        boolean bySensor = sensorId != null && !sensorId.isBlank();
        String sql;
        if (bySensor) {
            // 精确查询：deviceId + sensorId 双 tag 过滤
            sql = "SELECT ts, v FROM " + CommonUtils.quote(superTableName)
                    + " WHERE `sensorId` = ? AND `deviceId` = ? AND ts >= ? AND ts <= ? ORDER BY ts DESC";
        } else {
            // 仅按 deviceId 查询设备下全部传感器数据，带出 sensorId 以区分不同传感器
            sql = "SELECT `sensorId`, ts, v FROM " + CommonUtils.quote(superTableName)
                    + " WHERE `deviceId` = ? AND ts >= ? AND ts <= ? ORDER BY ts DESC";
        }
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            if (bySensor) {
                ps.setString(1, sensorId);
                ps.setString(2, deviceId);
                ps.setTimestamp(3, new Timestamp(startTime));
                ps.setTimestamp(4, new Timestamp(endTime));
                return getDataPoints(ps, sensorId);
            }
            ps.setString(1, deviceId);
            ps.setTimestamp(2, new Timestamp(startTime));
            ps.setTimestamp(3, new Timestamp(endTime));
            return getDataPoints(ps);
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 查询失败: " + (bySensor ? sensorId : deviceId), e);
        }
    }

    @Override
    public List<DataPoint> query(List<DataPoint> dataPoints, long startTime, long endTime) {
        if (dataPoints == null || dataPoints.isEmpty()) {
            throw new IllegalArgumentException("dataPoints 不能为空");
        }
        if (startTime > endTime) {
            throw new IllegalArgumentException("startTime 不能大于 endTime");
        }
        // 每个 (deviceId, sensorId) 对应唯一子表 s_<deviceId>_<sensorId>，用 tbname IN 一次往返精确命中全部子表
        List<String> tableNames = new ArrayList<>();
        for (DataPoint dataPoint : dataPoints) {
            if (dataPoint == null) {
                throw new IllegalArgumentException("dataPoints 中存在空元素");
            }
            String deviceId = dataPoint.getDeviceId();
            CommonUtils.requireNotBlank(deviceId, "deviceId");
            String sensorId = dataPoint.getSid();
            CommonUtils.requireNotBlank(sensorId, "sensorId");
            tableNames.add("s_" + deviceId + "_" + sensorId);
        }

        String placeholders = String.join(",", Collections.nCopies(tableNames.size(), "?"));
        String sql = "SELECT `sensorId`, `deviceId`, ts, v FROM " + CommonUtils.quote(superTableName)
                + " WHERE tbname IN (" + placeholders + ") AND ts >= ? AND ts <= ? ORDER BY ts DESC";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            int idx = 1;
            for (String tableName : tableNames) {
                ps.setString(idx++, tableName);
            }
            ps.setTimestamp(idx++, new Timestamp(startTime));
            ps.setTimestamp(idx, new Timestamp(endTime));
            try (ResultSet rs = ps.executeQuery()) {
                List<DataPoint> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(new DataPoint(rs.getString("sensorId"), rs.getString("deviceId"),
                            rs.getTimestamp("ts").getTime(), rs.getFloat("v")));
                }
                return result;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 批量查询失败", e);
        }
    }

    @Override
    public List<Map<String, Object>> query(String sql) {

        CommonUtils.requireNotBlank(sql, "sql");
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            List<Map<String, Object>> result = new ArrayList<>();
            while (rs.next()) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(meta.getColumnLabel(i), rs.getObject(i));
                }
                result.add(row);
            }
            return result;
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 查询失败: " + sql, e);
        }
    }

    @Override
    public void voidQuery(String sql) {
        CommonUtils.requireNotBlank(sql, "sql");
        try (Connection connection = dataSource.getConnection();
             Statement st = connection.createStatement()) {
            st.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 查询失败: " + sql, e);
        }
    }

    /**
     * 数据封装 - 单输出值查询
     *
     * @author ZhenYu Chi
     * @date 2026/8/25 09:07
     */
    private List<DataPoint> getDataPoints(PreparedStatement ps, String sensorId) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            List<DataPoint> result = new ArrayList<>();
            while (rs.next()) {
                result.add(new DataPoint(sensorId, rs.getTimestamp("ts").getTime(), rs.getFloat("v")));
            }
            return result;
        }
    }

    /**
     * 数据封装 - 多输出值查询
     *
     * @author ZhenYu Chi
     * @date 2026/8/25 09:07
     */
    private List<DataPoint> getDataPoints(PreparedStatement ps) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            List<DataPoint> result = new ArrayList<>();
            while (rs.next()) {
                result.add(new DataPoint(rs.getString("sensorId"),
                        rs.getTimestamp("ts").getTime(), rs.getFloat("v")));
            }
            return result;
        }
    }

    @Override
    public PageResult<DataPoint> queryPage(String deviceId, String sensorId, long startTime, long endTime, int page, int limit) {
        CommonUtils.requireNotBlank(deviceId, "deviceId");
        CommonUtils.requireNotBlank(sensorId, "sensorId");
        if (startTime > endTime) {
            throw new IllegalArgumentException("startTime 不能大于 endTime");
        }
        if (page < 1) {
            throw new IllegalArgumentException("page 必须从 1 开始");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("limit 必须大于 0");
        }

        try (Connection connection = dataSource.getConnection()) {
            long total = count(connection, deviceId, sensorId, startTime, endTime);
            if (total == 0) {
                return new PageResult<>(Collections.emptyList(), 0L, page, limit);
            }

            int offset = (page - 1) * limit;
            String sql = "SELECT ts, v FROM " + CommonUtils.quote(superTableName)
                    + " WHERE `deviceId` = ? AND `sensorId` = ? AND ts >= ? AND ts <= ? ORDER BY ts DESC LIMIT " + limit + " OFFSET " + offset;
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, deviceId);
                ps.setString(2, sensorId);
                ps.setTimestamp(3, new Timestamp(startTime));
                ps.setTimestamp(4, new Timestamp(endTime));
                return new PageResult<>(getDataPoints(ps, sensorId), total, page, limit);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 分页查询失败: " + sensorId, e);
        }
    }

    @Override
    public List<DataPoint> queryRealTime(String deviceId, String sensorId) {
        CommonUtils.requireNotBlank(deviceId, "deviceId");
        boolean bySensor = sensorId != null && !sensorId.isBlank();
        if (bySensor) {
            // 精确查询：deviceId + sensorId 双 tag 过滤，时间倒序取首条
            String sql = "SELECT ts, v FROM " + CommonUtils.quote(superTableName)
                    + " WHERE `sensorId` = ? AND `deviceId` = ? ORDER BY ts DESC LIMIT 1";
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, sensorId);
                ps.setString(2, deviceId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (!rs.next()) {
                        return Collections.emptyList();
                    }
                    DataPoint point = new DataPoint(sensorId, deviceId,
                            rs.getTimestamp("ts").getTime(), rs.getFloat("v"));
                    return Collections.singletonList(point);
                }
            } catch (SQLException e) {
                throw new IllegalStateException("TDengine 查询最新数据失败: " + sensorId, e);
            }
        }
        // 仅按 deviceId 查询设备下所有传感点的最新数据：last_row 取各子表最新一行，GROUP BY tbname 逐子表返回
        String sql = "SELECT last_row(ts), last_row(v), `sensorId` FROM " + CommonUtils.quote(superTableName)
                + " WHERE `deviceId` = ? GROUP BY tbname";
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, deviceId);
            try (ResultSet rs = ps.executeQuery()) {
                List<DataPoint> result = new ArrayList<>();
                while (rs.next()) {
                    long tm = rs.getTimestamp(1).getTime();
                    float v = rs.getFloat(2);
                    String sid = rs.getString(3);
                    result.add(new DataPoint(sid, deviceId, tm, v));
                }
                return result;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 查询设备最新数据失败: " + deviceId, e);
        }
    }

    /**
     * 统计 deviceId 下 sensorId 在 [startTime, endTime] 内的数据点总数。
     */
    private long count(Connection connection, String deviceId, String sensorId, long startTime, long endTime) throws SQLException {
        String sql = "SELECT COUNT(*) FROM " + CommonUtils.quote(superTableName)
                + " WHERE `deviceId` = ? AND `sensorId` = ? AND ts >= ? AND ts <= ?";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, deviceId);
            ps.setString(2, sensorId);
            ps.setTimestamp(3, new Timestamp(startTime));
            ps.setTimestamp(4, new Timestamp(endTime));
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    @Override
    public void write(DataPoint dataPoint) {
        CommonUtils.requireDataPoint(dataPoint);
        String sensorId = dataPoint.getSid();
        String deviceId = dataPoint.getDeviceId();
        CommonUtils.requireNotBlank(sensorId, "sensorId");
        CommonUtils.requireNotBlank(deviceId, "deviceId");

        // 单条写入用内联文本只需一次往返，能显著降低单点写入延迟。
        String sql = "INSERT INTO " + quoteSubTable(deviceId + "_" + sensorId)
                + " USING " + CommonUtils.quote(superTableName)
                + " TAGS ('" + sensorId + "', '" + deviceId + "')"
                + " VALUES (" + dataPoint.getTm() + ", " + dataPoint.getValue() + ")";
        try (Connection connection = dataSource.getConnection();
             Statement st = connection.createStatement()) {
            st.executeUpdate(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 写入数据点失败: " + sensorId, e);
        }
    }

    @Override
    public void batchWrite(List<DataPoint> dataPoints) {
        if (dataPoints == null || dataPoints.isEmpty()) {
            return;
        }
        // 每个数据点自带 sid，按 sid 分组后每组合并成一条多 VALUES 的 INSERT，再一次性提交
        Map<String, List<DataPoint>> bySid = new LinkedHashMap<>();
        for (DataPoint dataPoint : dataPoints) {
            CommonUtils.requireDataPoint(dataPoint);
            String sid = dataPoint.getSid();
            CommonUtils.requireNotBlank(sid, "sid");
            bySid.computeIfAbsent(sid, k -> new ArrayList<>()).add(dataPoint);
        }
        try (Connection connection = dataSource.getConnection();
             Statement st = connection.createStatement()) {
            for (Map.Entry<String, List<DataPoint>> entry : bySid.entrySet()) {
                String sensorId = entry.getKey();
                List<DataPoint> points = entry.getValue();
                String deviceId = points.get(0).getDeviceId();
                CommonUtils.requireNotBlank(deviceId, "deviceId");
                StringBuilder sql = new StringBuilder("INSERT INTO ")
                        .append(quoteSubTable(deviceId + "_" + sensorId)).append(" USING ").append(CommonUtils.quote(superTableName))
                        .append(" TAGS ('").append(sensorId).append("', '").append(deviceId).append("') VALUES ");
                for (int i = 0; i < points.size(); i++) {
                    DataPoint dataPoint = points.get(i);
                    if (i > 0) {
                        sql.append(' ');
                    }
                    sql.append('(').append(dataPoint.getTm()).append(", ").append(dataPoint.getValue()).append(')');
                }
                st.addBatch(sql.toString());
            }
            st.executeBatch();
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 批量写入数据点失败", e);
        }
    }

    @Override
    public void delete(String deviceId, String sensorId, long startTime, long endTime) {
        CommonUtils.requireNotBlank(deviceId, "deviceId");
        if (startTime > endTime) {
            throw new IllegalArgumentException("startTime 不能大于 endTime");
        }
        boolean bySensor = sensorId != null && !sensorId.isBlank();
        String sql;
        if (bySensor) {
            // 精确删除：deviceId + sensorId 双 tag 过滤
            sql = "DELETE FROM " + CommonUtils.quote(superTableName)
                    + " WHERE `sensorId` = ? AND `deviceId` = ? AND ts >= ? AND ts <= ?";
        } else {
            // 仅按 deviceId 删除设备下全部传感器数据
            sql = "DELETE FROM " + CommonUtils.quote(superTableName)
                    + " WHERE `deviceId` = ? AND ts >= ? AND ts <= ?";
        }
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(sql)) {
            if (bySensor) {
                ps.setString(1, sensorId);
                ps.setString(2, deviceId);
                ps.setTimestamp(3, new Timestamp(startTime));
                ps.setTimestamp(4, new Timestamp(endTime));
            } else {
                ps.setString(1, deviceId);
                ps.setTimestamp(2, new Timestamp(startTime));
                ps.setTimestamp(3, new Timestamp(endTime));
            }
            ps.execute();
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 删除失败: " + (bySensor ? sensorId : deviceId), e);
        }
    }

    @Override
    public void batchDelete(List<Map<String, Object>> deleteList) {
        if (deleteList == null || deleteList.isEmpty()) {
            return;
        }
        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement()) {
            for (Map<String, Object> item : deleteList) {
                String deviceId = ConfigUtils.getString(item, "deviceId");
                CommonUtils.requireNotBlank(deviceId, "deviceId");
                String id = ConfigUtils.getString(item, "id");
                boolean bySensor = id != null && !id.isBlank();
                long sTm = ConfigUtils.getLong(item, 0L, "sTm");
                long eTm = ConfigUtils.getLong(item, 0L, "eTm");
                if (sTm > eTm) {
                    throw new IllegalArgumentException("sTm 不能大于 eTm");
                }
                StringBuilder deleteSql = new StringBuilder("DELETE FROM ").append(CommonUtils.quote(superTableName));
                if (bySensor) {
                    deleteSql.append(" WHERE `deviceId` = '").append(deviceId)
                            .append("' AND `sensorId` = '").append(id).append("'");
                } else {
                    deleteSql.append(" WHERE `deviceId` = '").append(deviceId).append("'");
                }
                deleteSql.append(" AND ts >= ").append(sTm).append(" AND ts <= ").append(eTm);
                stmt.addBatch(deleteSql.toString());
            }
            stmt.executeBatch();
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 批量删除失败", e);
        }
    }

    @Override
    public void close() {
        if (dataSource instanceof AutoCloseable) {
            try {
                ((AutoCloseable) dataSource).close();
            } catch (Exception e) {
                throw new IllegalStateException("关闭 TDengine 连接池失败", e);
            }
        }
    }

    /**
     * 对子表标识符进行 SQL 转义，添加反引号。
     *
     * @param identifier 子表标识符
     * @return
     */
    private static String quoteSubTable(String identifier) {
        CommonUtils.requireNotBlank(identifier, "identifier");
        return CommonUtils.quote("s_" + identifier);
    }
}
