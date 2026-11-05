package com.bestdata.em.tsoss.tdengine;

import com.bestdata.em.tsoss.api.DataPoint;
import com.bestdata.em.tsoss.api.PageResult;
import com.bestdata.em.tsoss.api.TimeSeriesClient;
import com.bestdata.em.tsoss.core.CommonUtils;
import com.bestdata.em.tsoss.core.ConfigUtils;
import com.bestdata.em.tsoss.tdengine.utils.TdengineSqlBuilder;
import com.bestdata.em.tsoss.tdengine.utils.TdengineUtil;
import com.bestdata.em.tsoss.tdengine.utils.TdengineValidator;
import com.bestdata.em.tsoss.tdengine.utils.TdengineWindowResolver;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TDengine 连接句柄，基于连接池（HikariCP）按次借还 JDBC。
 *
 * <p>
 * 每次操作从池中借用连接、用完即还，因此多线程并发读写天然安全；断线检测与重建由连接池
 * 在借出时完成，业务方无需关注连接状态。
 * </p>
 */
public class TdengineClient implements TimeSeriesClient, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TdengineClient.class);

    /**
     * 单条 SQL 中 tbname IN 列表的最大长度。子表过多时分批查询后再合并，
     * 避免 SQL 过长以及引擎对 IN 列表/参数个数的限制。
     */
    private static final int TBNAME_IN_BATCH_SIZE = 50;

    private final DataSource dataSource;
    private final String superTableName;

    /** 聚合窗口对齐使用的时区，窗口边界会对齐到该时区的 00:00。 */
    private final ZoneId zoneId;

    /**
     * 服务端是否支持 COLS 函数flag，null 表示尚未获取。获取成功后缓存；获取本身失败（如取连接异常）
     * 不写缓存，留待下次重试。
     */
    private volatile Boolean colsSupported;

    TdengineClient(DataSource dataSource, String superTableName, ZoneId zoneId) {
        this.dataSource = dataSource;
        this.superTableName = superTableName;
        this.zoneId = zoneId == null ? TdengineWindowResolver.DEFAULT_ZONE_ID : zoneId;
    }

    // ==================== 连接 ====================

    @Override
    public boolean isConnected() {
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(TdengineSqlBuilder.selectServerVersion())) {
            return rs.next();
        } catch (SQLException e) {
            log.debug("TDengine 连通性检测失败", e);
            return false;
        }
    }

    // ==================== 查询 ====================

    @Override
    public List<DataPoint> query(String deviceId, String sensorId, long startTime, long endTime) {
        TdengineValidator.requireDeviceId(deviceId);
        TdengineValidator.requireTimeRange(startTime, endTime);

        // 先使用超级表查询，如果发现后期查询效率慢，改变查询方式，直接查询子表名
        boolean bySensor = TdengineValidator.isNotBlank(sensorId);
        String sql = bySensor
                ? TdengineSqlBuilder.selectBySensor(superTableName)
                : TdengineSqlBuilder.selectByDevice(superTableName);

        try (Connection connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement(sql)) {
            // 是 JDBC 预编译语句的参数下标，用来告诉 PreparedStatement 当前该往第几个 ? 塞值
            int index = 1;
            if (bySensor) {
                ps.setString(index++, sensorId);
            }
            ps.setString(index++, deviceId);
            TdengineUtil.bindTimeRange(ps, index, startTime, endTime);
            return bySensor
                    ? TdengineUtil.getDataPoints(ps, sensorId)
                    : TdengineUtil.getDataPoints(ps);
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 查询失败: " + (bySensor ? sensorId : deviceId), e);
        }
    }

    @Override
    public List<DataPoint> query(List<DataPoint> dataPoints, long startTime, long endTime) {
        TdengineValidator.requireDataPoints(dataPoints);
        TdengineValidator.requireTimeRange(startTime, endTime);

        // 每个 (deviceId, sensorId) 对应唯一子表 s_<deviceId>_<sensorId>，
        // 用 tbname IN 一次往返精确命中全部子表（去重避免重复扫描）
        List<String> tableNames = TdengineUtil.collectTableNames(dataPoints);
        String sql = TdengineSqlBuilder.selectByTables(superTableName, tableNames.size());

        try (Connection connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement(sql)) {
            int index = TdengineUtil.bindTableNames(ps, tableNames, 1);
            TdengineUtil.bindTimeRange(ps, index, startTime, endTime);
            return TdengineUtil.getDeviceDataPoints(ps);
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 批量查询失败", e);
        }
    }

    @Override
    public List<DataPoint> queryAggregation(List<DataPoint> dataPoints, long startTime, long endTime, String window,
            String way) {
        TdengineValidator.requireDataPoints(dataPoints);
        TdengineValidator.requireTimeRange(startTime, endTime);
        TdengineValidator.requireWindow(window);
        String aggregation = TdengineValidator.requireAggregation(way);
        // 把 6h 这类窗口补上 offset，使窗口边界对齐配置时区的 00:00
        String windowSpec = TdengineWindowResolver.toIntervalSpec(window, zoneId);

        // 同一 (deviceId, sensorId) 对应唯一子表，去重后按批查询，避免 IN 列表过长
        List<String> tableNames = TdengineUtil.collectTableNames(dataPoints);

        // 窗口按固定 offset 对齐，与批次划分无关，分批聚合与单条查询结果一致；
        // 全程复用同一连接，仅按批增加往返次数
        try (Connection connection = dataSource.getConnection()) {
            List<DataPoint> result = new ArrayList<>();
            for (List<String> batch : TdengineUtil.partition(tableNames, TBNAME_IN_BATCH_SIZE)) {
                result.addAll(queryAggregationBatch(connection, batch, startTime, endTime, windowSpec, aggregation));
            }
            // 与其它查询保持一致，按时间倒序返回
            result.sort(Comparator.comparingLong(DataPoint::getTm).reversed());
            return result;
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 聚合查询失败", e);
        }
    }

    /**
     * 对单批子表执行窗口聚合。PARTITION BY tbname 保证各传感点独立开窗，结果点的时间列由
     * {@link #timeExpression(String)} 按聚合方式决定。
     */
    private List<DataPoint> queryAggregationBatch(Connection connection, List<String> tableNames, long startTime,
            long endTime, String windowSpec, String aggregation) throws SQLException {
        try {
            return executeAggregationBatch(connection, tableNames, startTime, endTime, windowSpec, aggregation,
                    timeExpression(aggregation));
        } catch (SQLException e) {
            // COLS 与窗口聚合的组合不被支持时，max/min 回退为窗口开始时间，保证查询仍可用
            if (!TdengineUtil.isExtremum(aggregation) || !supportsCols()) {
                throw e;
            }
            colsSupported = Boolean.FALSE;
            log.warn("TDengine 不支持 COLS 聚合时间列，{} 的时间回退为窗口开始时间", aggregation, e);
            return executeAggregationBatch(connection, tableNames, startTime, endTime, windowSpec, aggregation,
                    TdengineUtil.WINDOW_START_EXPR);
        }
    }

    /**
     * 以指定的时间列执行一次窗口聚合查询。
     */
    private List<DataPoint> executeAggregationBatch(Connection connection, List<String> tableNames, long startTime,
            long endTime, String windowSpec, String aggregation, String timeExpression) throws SQLException {
        String sql = TdengineSqlBuilder.selectAggregation(superTableName, tableNames.size(), timeExpression,
                aggregation, windowSpec);
        log.debug("TDengine 聚合查询 SQL: {}", sql);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int index = TdengineUtil.bindTableNames(ps, tableNames, 1);
            TdengineUtil.bindTimeRange(ps, index, startTime, endTime);
            return TdengineUtil.getAggregationDataPoints(ps);
        }
    }

    /**
     * 按聚合方式决定结果点使用的时间列：
     * <ul>
     * <li>avg：窗口开始时间 {@code _wstart}，均值不属于任何一条记录；</li>
     * <li>first / last：窗口内首条、末条记录的真实时间；</li>
     * <li>max / min：极值所在记录的真实时间，由 COLS 函数取回（需服务端 3.3.6.0 及以上），
     * 否则回退为窗口开始时间。</li>
     * </ul>
     */
    private String timeExpression(String aggregation) {
        return switch (aggregation) {
            case "avg" -> TdengineUtil.WINDOW_START_EXPR;
            case "first" -> "FIRST(ts)";
            case "last" -> "LAST(ts)";
            case "max", "min" -> supportsCols()
                    ? "COLS(" + aggregation.toUpperCase(Locale.ROOT) + "(v), ts)"
                    : TdengineUtil.WINDOW_START_EXPR;
            default -> throw new IllegalArgumentException("不支持的聚合方式: " + aggregation);
        };
    }

    /**
     * 探测服务端是否支持 COLS 函数（3.3.6.0 及以上）。结果缓存，避免每次聚合都查一次版本。
     */
    private boolean supportsCols() {
        Boolean cached = colsSupported;
        if (cached != null) {
            return cached;
        }
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(TdengineSqlBuilder.selectServerVersion())) {
            if (!rs.next()) {
                return false;
            }
            String version = rs.getString(1);
            boolean supported = TdengineUtil.isVersionAtLeast(version);
            colsSupported = supported;
            if (!supported) {
                log.warn("TDengine 服务端版本 {} 低于 3.3.6.0，max/min 聚合的时间回退为窗口开始时间", version);
            }
            return supported;
        } catch (SQLException e) {
            // 探测失败不写缓存，下次聚合时重试
            log.warn("探测 TDengine 服务端版本失败，{}", e.getMessage());
            return false;
        }
    }

    @Override
    public List<Map<String, Object>> query(String sql) {
        CommonUtils.requireNotBlank(sql, "sql");
        try (Connection connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement(sql);
                ResultSet rs = ps.executeQuery()) {
            return readRows(rs);
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 查询失败: " + sql, e);
        }
    }

    @Override
    public void voidQuery(String sql) {
        CommonUtils.requireNotBlank(sql, "sql");
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 查询失败: " + sql, e);
        }
    }

    @Override
    public PageResult<DataPoint> queryPage(String deviceId, String sensorId, long startTime, long endTime, int page,
            int limit) {
        TdengineValidator.requireDeviceId(deviceId);
        TdengineValidator.requireSensorId(sensorId);
        TdengineValidator.requireTimeRange(startTime, endTime);
        TdengineValidator.requirePage(page, limit);

        try (Connection connection = dataSource.getConnection()) {
            long total = count(connection, deviceId, sensorId, startTime, endTime);
            if (total == 0) {
                return new PageResult<>(Collections.emptyList(), 0L, page, limit);
            }
            List<DataPoint> rows = queryPageRows(connection, deviceId, sensorId, startTime, endTime, page, limit);
            return new PageResult<>(rows, total, page, limit);
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 分页查询失败: " + sensorId, e);
        }
    }

    /**
     * 统计 deviceId 下 sensorId 在 [startTime, endTime] 内的数据点总数。
     */
    private long count(Connection connection, String deviceId, String sensorId, long startTime, long endTime)
            throws SQLException {
        String sql = TdengineSqlBuilder.countBySensor(superTableName);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, deviceId);
            ps.setString(2, sensorId);
            TdengineUtil.bindTimeRange(ps, 3, startTime, endTime);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    /**
     * 查询指定页的数据行，按时间倒序。
     */
    private List<DataPoint> queryPageRows(Connection connection, String deviceId, String sensorId, long startTime,
            long endTime, int page, int limit) throws SQLException {
        int offset = (page - 1) * limit;
        String sql = TdengineSqlBuilder.selectPage(superTableName, limit, offset);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, deviceId);
            ps.setString(2, sensorId);
            TdengineUtil.bindTimeRange(ps, 3, startTime, endTime);
            return TdengineUtil.getDataPoints(ps, sensorId);
        }
    }

    @Override
    public List<DataPoint> queryRealTime(String deviceId, String sensorId) {
        TdengineValidator.requireDeviceId(deviceId);
        return TdengineValidator.isNotBlank(sensorId)
                ? queryLatestBySensor(deviceId, sensorId)
                : queryLatestByDevice(deviceId);
    }

    /**
     * 精确查询：deviceId + sensorId 双 tag 过滤，时间倒序取首条。
     */
    private List<DataPoint> queryLatestBySensor(String deviceId, String sensorId) {
        String sql = TdengineSqlBuilder.selectLatestBySensor(superTableName);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, sensorId);
            ps.setString(2, deviceId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    return Collections.emptyList();
                }
                return Collections.singletonList(new DataPoint(sensorId, deviceId,
                        rs.getTimestamp("ts").getTime(), rs.getFloat("v")));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 查询最新数据失败: " + sensorId, e);
        }
    }

    /**
     * 仅按 deviceId 查询设备下所有传感点的最新数据：last_row 取各子表最新一行，
     * GROUP BY tbname 逐子表返回。
     */
    private List<DataPoint> queryLatestByDevice(String deviceId) {
        String sql = TdengineSqlBuilder.selectLatestByDevice(superTableName);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, deviceId);
            try (ResultSet rs = ps.executeQuery()) {
                List<DataPoint> result = new ArrayList<>();
                while (rs.next()) {
                    result.add(new DataPoint(rs.getString(3), deviceId,
                            rs.getTimestamp(1).getTime(), rs.getFloat(2)));
                }
                return result;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 查询设备最新数据失败: " + deviceId, e);
        }
    }

    /**
     * 把结果集按「列名 → 值」逐行展开。
     */
    private static List<Map<String, Object>> readRows(ResultSet rs) throws SQLException {
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
    }

    // ==================== 写入 ====================

    @Override
    public void write(DataPoint dataPoint) {
        CommonUtils.requireDataPoint(dataPoint);
        TdengineValidator.requireSensorId(dataPoint.getSid());
        TdengineValidator.requireDeviceId(dataPoint.getDeviceId());

        String sql = TdengineSqlBuilder.insertOne(superTableName, dataPoint);
        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 写入数据点失败: " + dataPoint.getSid(), e);
        }
    }

    @Override
    public void batchWrite(List<DataPoint> dataPoints) {
        if (dataPoints == null || dataPoints.isEmpty()) {
            return;
        }
        Collection<List<DataPoint>> groups = groupByDeviceAndSensor(dataPoints);
        String sql = TdengineSqlBuilder.insertMultiple(superTableName, groups);

        try (Connection connection = dataSource.getConnection();
                Statement statement = connection.createStatement()) {
            if (log.isDebugEnabled()) {
                log.debug("TDengine 批量写入 {} 组、{} 个数据点", groups.size(), dataPoints.size());
            }
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 批量写入数据点失败", e);
        }
    }

    /**
     * 按 (deviceId, sensorId) 分组：每个组合对应一张子表，组内数据可合并成一条多 VALUES 的 INSERT。
     */
    private static Collection<List<DataPoint>> groupByDeviceAndSensor(List<DataPoint> dataPoints) {
        Map<String, List<DataPoint>> groups = new LinkedHashMap<>();
        for (DataPoint dataPoint : dataPoints) {
            CommonUtils.requireDataPoint(dataPoint);
            String sensorId = dataPoint.getSid();
            String deviceId = dataPoint.getDeviceId();
            TdengineValidator.requireSensorId(sensorId);
            TdengineValidator.requireDeviceId(deviceId);
            groups.computeIfAbsent(deviceId + "_" + sensorId, key -> new ArrayList<>()).add(dataPoint);
        }
        return groups.values();
    }

    // ==================== 删除 ====================

    @Override
    public void delete(String deviceId, String sensorId, long startTime, long endTime) {
        TdengineValidator.requireDeviceId(deviceId);
        TdengineValidator.requireTimeRange(startTime, endTime);

        boolean bySensor = TdengineValidator.isNotBlank(sensorId);
        String sql = bySensor
                ? TdengineSqlBuilder.deleteBySensor(superTableName)
                : TdengineSqlBuilder.deleteByDevice(superTableName);

        try (Connection connection = dataSource.getConnection();
                PreparedStatement ps = connection.prepareStatement(sql)) {
            int index = 1;
            if (bySensor) {
                ps.setString(index++, sensorId);
            }
            ps.setString(index++, deviceId);
            ps.setLong(index++, startTime);
            ps.setLong(index, endTime);
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
                Statement statement = connection.createStatement()) {
            for (Map<String, Object> item : deleteList) {
                statement.addBatch(buildBatchDeleteSql(superTableName, item));
            }
            statement.executeBatch();
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 批量删除失败", e);
        }
    }

    /**
     * 解析单个批量删除项并拼装 DELETE 语句。
     */
    private static String buildBatchDeleteSql(String superTableName, Map<String, Object> item) {
        String deviceId = ConfigUtils.getString(item, "deviceId");
        TdengineValidator.requireDeviceId(deviceId);
        String sensorId = ConfigUtils.getString(item, "id");
        long startTime = ConfigUtils.getLong(item, 0L, "sTm");
        long endTime = ConfigUtils.getLong(item, 0L, "eTm");
        TdengineValidator.requireDeleteTimeRange(startTime, endTime);
        return TdengineSqlBuilder.deleteByDeviceAndSensor(superTableName, deviceId, sensorId, startTime, endTime);
    }

    // ==================== 生命周期 ====================

    @Override
    public void close() {
        if (dataSource instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                throw new IllegalStateException("关闭 TDengine 连接池失败", e);
            }
        }
    }
}
