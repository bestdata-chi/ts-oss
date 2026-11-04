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
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.bestdata.em.tsoss.tdengine.utils.TdengineUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TDengine 连接句柄，基于连接池（HikariCP）按次借还 JDBC {@link Connection}。
 *
 * <p>
 * 每次操作从池中借用连接、用完即还，因此多线程并发读写天然安全；断线检测与重建由连接池
 * 在借出时完成，业务方无需关注连接状态。
 * </p>
 */
public class TdengineClient implements TimeSeriesClient, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TdengineClient.class);

    /** 支持的聚合方式，作为白名单校验后再拼入 SQL，避免注入。 */
    private static final Set<String> AGGREGATION_WAYS = Set.of("avg", "max", "min", "first", "last");

    /** 窗口格式：正整数 + 单位（s/m/h/d），如 1m、10m、6h、1d。 */
    private static final Pattern WINDOW_PATTERN = Pattern.compile("^[1-9]\\d*[smhd]$");

    /**
     * 单条 SQL 中 tbname IN 列表的最大长度。子表过多时分批查询后再合并，
     * 避免 SQL 过长以及引擎对 IN 列表/参数个数的限制。
     */
    private static final int TBNAME_IN_BATCH_SIZE = 50;

    private final DataSource dataSource;
    private final String superTableName;

    /**
     * 服务端是否支持 COLS 函数，null 表示尚未获取。获取成功后缓存；获取本身失败（如取连接异常）
     * 不写缓存，留待下次重试。
     */
    private volatile Boolean colsSupported;

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
                return TdengineUtil.getDataPoints(ps, sensorId);
            }
            ps.setString(1, deviceId);
            ps.setTimestamp(2, new Timestamp(startTime));
            ps.setTimestamp(3, new Timestamp(endTime));
            return TdengineUtil.getDataPoints(ps);
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
        // 每个 (deviceId, sensorId) 对应唯一子表 s_<deviceId>_<sensorId>，用 tbname IN
        // 一次往返精确命中全部子表（去重避免重复扫描）
        List<String> tableNames = TdengineUtil.collectTableNames(dataPoints);

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
    public List<DataPoint> queryAggregation(List<DataPoint> dataPoints, long startTime, long endTime, String window,
            String way) {
        if (dataPoints == null || dataPoints.isEmpty()) {
            throw new IllegalArgumentException("dataPoints 不能为空");
        }
        if (startTime > endTime) {
            throw new IllegalArgumentException("startTime 不能大于 endTime");
        }
        if (window == null || !WINDOW_PATTERN.matcher(window).matches()) {
            throw new IllegalArgumentException(
                    "window 格式非法，应为 \"1m\"、\"10m\"、\"30m\"、\"1h\"、\"6h\"、\"12h\"、\"1d\" 等: " + window);
        }
        String aggregation = way == null ? null : way.toLowerCase(Locale.ROOT);
        if (aggregation == null || !AGGREGATION_WAYS.contains(aggregation)) {
            throw new IllegalArgumentException("way 仅支持 avg、max、min、first、last: " + way);
        }

        // 同一 (deviceId, sensorId) 对应唯一子表，去重后按批查询，避免 IN 列表过长
        List<String> tableNames = TdengineUtil.collectTableNames(dataPoints);

        // 窗口对齐与批次划分无关（按 epoch 对齐），分批聚合与单条查询结果一致；
        // 全程复用同一连接，仅按批增加往返次数
        try (Connection connection = dataSource.getConnection()) {
            List<DataPoint> result = new ArrayList<>();
            for (int from = 0; from < tableNames.size(); from += TBNAME_IN_BATCH_SIZE) {
                List<String> batch = tableNames.subList(from,
                        Math.min(from + TBNAME_IN_BATCH_SIZE, tableNames.size()));
                result.addAll(queryAggregationBatch(connection, batch, startTime, endTime, window, aggregation));
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
     */
    private List<DataPoint> queryAggregationBatch(Connection connection, List<String> tableNames, long startTime,
            long endTime, String window, String aggregation) throws SQLException {
        try {
            return executeAggregationBatch(connection, tableNames, startTime, endTime, window, aggregation,
                    timeExpression(aggregation));
        } catch (SQLException e) {
            // COLS 与窗口聚合的组合不被支持时，max/min 回退为窗口开始时间，保证查询仍可用
            if (TdengineUtil.isExtremum(aggregation) && supportsCols()) {
                colsSupported = Boolean.FALSE;
                log.warn("TDengine 不支持 COLS 聚合时间列，{} 的时间回退为窗口开始时间", aggregation, e);
                return executeAggregationBatch(connection, tableNames, startTime, endTime, window, aggregation,
                        TdengineUtil.WINDOW_START_EXPR);
            }
            throw e;
        }
    }

    /**
     * 以指定的时间列执行一次窗口聚合查询，时间列统一别名为 {@code ts_time}。
     */
    private List<DataPoint> executeAggregationBatch(Connection connection, List<String> tableNames, long startTime,
            long endTime, String window, String aggregation, String timeExpression) throws SQLException {
        String placeholders = String.join(",", Collections.nCopies(tableNames.size(), "?"));
        String sql = "SELECT `sensorId`, `deviceId`, " + timeExpression + " AS ts_time, " + aggregation
                + "(v) AS v FROM " + CommonUtils.quote(superTableName)
                + " WHERE tbname IN (" + placeholders + ") AND ts >= ? AND ts <= ?"
                + " PARTITION BY tbname INTERVAL(" + window + ")";
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            int idx = 1;
            for (String tableName : tableNames) {
                ps.setString(idx++, tableName);
            }
            ps.setTimestamp(idx++, new Timestamp(startTime));
            ps.setTimestamp(idx, new Timestamp(endTime));
            try (ResultSet rs = ps.executeQuery()) {
                List<DataPoint> result = new ArrayList<>();
                while (rs.next()) {
                    Timestamp tm = rs.getTimestamp("ts_time");
                    Object rawValue = rs.getObject("v");
                    if (tm == null || rawValue == null) {
                        continue;
                    }
                    result.add(new DataPoint(rs.getString("sensorId"), rs.getString("deviceId"),
                            tm.getTime(), toFloat(rawValue)));
                }
                return result;
            }
        }
    }

    /**
     * 把聚合结果值统一转成 float。
     *
     * <p>
     * 聚合结果的实际类型随函数而变：avg 返回 DOUBLE（驱动里是 Double），first/last/max/min
     * 返回 FLOAT（Float），UBIGINT 系为 BigDecimal。taos-jdbcdriver 的 getFloat/getDouble 会用
     * {@code BigDecimal.compareTo(MIN_VALUE)} 做范围校验，值为 0 或负数时误判越界并抛
     * {@code 0x2314 ... outside valid range for the jdbcType}，因此这里走不做校验的 getObject，
     * 再按 Number 自行收窄。
     * </p>
     */
    private static float toFloat(Object raw) {
        if (raw instanceof Number number) {
            return number.floatValue();
        }
        return Float.parseFloat(raw.toString());
    }

    /**
     * 按聚合方式决定结果点使用的时间列：
     * <ul>
     * <li>avg：窗口结束时间 {@code _wend}，均值不属于任何一条记录；</li>
     * <li>first / last：窗口内首条、末条记录的真实时间；</li>
     * <li>max / min：极值所在记录的真实时间，由 COLS 函数取回（需服务端 3.3.6.0 及以上），
     * 否则回退为窗口开始时间。</li>
     * </ul>
     */
    private String timeExpression(String aggregation) {
        return switch (aggregation) {
            case "avg" -> TdengineUtil.WINDOW_END_EXPR;
            case "first" -> "FIRST(ts)";
            case "last" -> "LAST(ts)";
            case "max", "min" -> supportsCols()
                    ? "COLS(" + aggregation.toUpperCase(Locale.ROOT) + "(v), ts)"
                    : TdengineUtil.WINDOW_START_EXPR;
            default -> throw new IllegalArgumentException("不支持的聚合方式: " + aggregation);
        };
    }

    /**
     * 获取服务端是否支持 COLS 函数（3.3.6.0 及以上）。结果缓存，避免每次聚合都查一次版本。
     */
    private boolean supportsCols() {
        Boolean cached = colsSupported;
        if (cached != null) {
            return cached;
        }
        try (Connection connection = dataSource.getConnection();
                Statement st = connection.createStatement();
                ResultSet rs = st.executeQuery("SELECT SERVER_VERSION()")) {
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

    @Override
    public PageResult<DataPoint> queryPage(String deviceId, String sensorId, long startTime, long endTime, int page,
            int limit) {
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
                    + " WHERE `deviceId` = ? AND `sensorId` = ? AND ts >= ? AND ts <= ? ORDER BY ts DESC LIMIT " + limit
                    + " OFFSET " + offset;
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, deviceId);
                ps.setString(2, sensorId);
                ps.setTimestamp(3, new Timestamp(startTime));
                ps.setTimestamp(4, new Timestamp(endTime));
                return new PageResult<>(TdengineUtil.getDataPoints(ps, sensorId), total, page, limit);
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
    private long count(Connection connection, String deviceId, String sensorId, long startTime, long endTime)
            throws SQLException {
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
        String sql = "INSERT INTO " + TdengineUtil.quoteSubTable(deviceId + "_" + sensorId)
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
        // 每个数据点自带 sid 与 deviceId，按 (deviceId, sid) 分组后每组合并成一条多 VALUES 的 INSERT，再一次性提交
        Map<String, List<DataPoint>> byDeviceAndSid = new LinkedHashMap<>();
        for (DataPoint dataPoint : dataPoints) {
            CommonUtils.requireDataPoint(dataPoint);
            String sid = dataPoint.getSid();
            CommonUtils.requireNotBlank(sid, "sid");
            String deviceId = dataPoint.getDeviceId();
            CommonUtils.requireNotBlank(deviceId, "deviceId");
            byDeviceAndSid.computeIfAbsent(deviceId + "_" + sid, k -> new ArrayList<>()).add(dataPoint);
        }
        try (Connection connection = dataSource.getConnection();
                Statement st = connection.createStatement()) {
            StringBuilder sql = new StringBuilder();
            for (List<DataPoint> points : byDeviceAndSid.values()) {
                String deviceId = points.get(0).getDeviceId();
                String sensorId = points.get(0).getSid();
                if (sql.length() == 0) {
                    sql.append("INSERT INTO ");
                } else {
                    sql.append(' ');
                }
                sql.append(TdengineUtil.quoteSubTable(deviceId + "_" + sensorId)).append(" USING ")
                        .append(CommonUtils.quote(superTableName))
                        .append(" TAGS ('").append(sensorId).append("', '").append(deviceId).append("') VALUES ");
                for (int i = 0; i < points.size(); i++) {
                    DataPoint dataPoint = points.get(i);
                    if (i > 0) {
                        sql.append(", ");
                    }
                    sql.append('(').append(dataPoint.getTm()).append(", ").append(dataPoint.getValue()).append(')');
                }
            }
            log.info("TDengine 批量写入数据点: {}", sql);
            st.executeUpdate(sql.toString());
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 批量写入数据点失败!", e);
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
                ps.setLong(3, startTime);
                ps.setLong(4, endTime);
            } else {
                ps.setString(1, deviceId);
                ps.setLong(2, startTime);
                ps.setLong(3, endTime);
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
                    throw new IllegalArgumentException("sTm 不能大于 eTm!");
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

}
