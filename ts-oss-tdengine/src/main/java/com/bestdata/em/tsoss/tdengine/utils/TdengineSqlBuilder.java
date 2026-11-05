package com.bestdata.em.tsoss.tdengine.utils;

import com.bestdata.em.tsoss.api.DataPoint;
import com.bestdata.em.tsoss.core.CommonUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * TDengine SQL 文本拼装工具。
 *
 * <p>
 * 只负责把已通过 {@link TdengineValidator} 校验的参数拼成 SQL 字符串，不持有状态、不做 IO。
 * 查询条件的值一律使用占位符 {@code ?}；写入与批量删除因 TDengine 语法限制（多表插入、
 * TAGS 子句、LIMIT/OFFSET 不支持占位符）才内联字面量，这些位置只接受已校验的数值与标识符。
 * </p>
 */
public final class TdengineSqlBuilder {

    /** 聚合查询结果中时间列的别名，读取结果时与 {@link #TIME_COLUMN_ALIAS} 保持一致。 */
    public static final String TIME_COLUMN_ALIAS = "ts_time";

    private static final String SENSOR_ID_COLUMN = "`sensorId`";
    private static final String DEVICE_ID_COLUMN = "`deviceId`";
    private static final String TBNAME = "tbname";

    private TdengineSqlBuilder() {
    }

    /**
     * 生成 count 个以逗号分隔的占位符，如 count=3 时返回 "?,?,?"。
     */
    public static String placeholders(int count) {
        return String.join(",", Collections.nCopies(count, "?"));
    }

    /**
     * 服务端版本查询，用于连通性检测与 COLS 支持探测。
     */
    public static String selectServerVersion() {
        return "SELECT SERVER_VERSION()";
    }

    /**
     * 按 deviceId + sensorId 精确查询原始数据。
     */
    public static String selectBySensor(String superTable) {
        return "SELECT ts, v FROM " + CommonUtils.quote(superTable)
                + " WHERE " + SENSOR_ID_COLUMN + " = ? AND " + DEVICE_ID_COLUMN + " = ?"
                + " AND ts >= ? AND ts <= ? ORDER BY ts DESC";
    }

    /**
     * 仅按 deviceId 查询设备下全部传感点数据，带出 sensorId 以区分不同传感点。
     */
    public static String selectByDevice(String superTable) {
        return "SELECT " + SENSOR_ID_COLUMN + ", ts, v FROM " + CommonUtils.quote(superTable)
                + " WHERE " + DEVICE_ID_COLUMN + " = ? AND ts >= ? AND ts <= ? ORDER BY ts DESC";
    }

    /**
     * 按子表名列表批量查询，tbname IN 占位符数量与子表数一致。
     */
    public static String selectByTables(String superTable, int tableCount) {
        return "SELECT " + SENSOR_ID_COLUMN + ", " + DEVICE_ID_COLUMN + ", ts, v FROM "
                + CommonUtils.quote(superTable)
                + " WHERE " + TBNAME + " IN (" + placeholders(tableCount) + ")"
                + " AND ts >= ? AND ts <= ? ORDER BY ts DESC";
    }

    /**
     * 窗口聚合查询：PARTITION BY tbname 保证各传感点独立开窗，时间列由调用方指定。
     *
     * @param timeExpression 时间列表达式，如 _wstart、FIRST(ts)、COLS(MAX(v), ts)
     * @param aggregation    聚合方式，已通过白名单校验
     * @param windowSpec     窗口描述，由 {@link TdengineWindowResolver#toIntervalSpec} 生成，
     *                       形如 {@code 6h} 或 {@code 6h, 4h}
     */
    public static String selectAggregation(String superTable, int tableCount, String timeExpression,
            String aggregation, String windowSpec) {
        return "SELECT " + SENSOR_ID_COLUMN + ", " + DEVICE_ID_COLUMN + ", " + timeExpression
                + " AS " + TIME_COLUMN_ALIAS + ", " + aggregation + "(v) AS v FROM "
                + CommonUtils.quote(superTable)
                + " WHERE " + TBNAME + " IN (" + placeholders(tableCount) + ")"
                + " AND ts >= ? AND ts <= ? PARTITION BY " + TBNAME + " INTERVAL(" + windowSpec + ")";
    }

    /**
     * 统计单个传感点在时间区间内的数据点总数。
     */
    public static String countBySensor(String superTable) {
        return "SELECT COUNT(*) FROM " + CommonUtils.quote(superTable)
                + " WHERE " + DEVICE_ID_COLUMN + " = ? AND " + SENSOR_ID_COLUMN + " = ?"
                + " AND ts >= ? AND ts <= ?";
    }

    /**
     * 分页查询。LIMIT/OFFSET 已通过分页校验，TDengine 不支持占位符，故内联。
     */
    public static String selectPage(String superTable, int limit, int offset) {
        return "SELECT ts, v FROM " + CommonUtils.quote(superTable)
                + " WHERE " + DEVICE_ID_COLUMN + " = ? AND " + SENSOR_ID_COLUMN + " = ?"
                + " AND ts >= ? AND ts <= ? ORDER BY ts DESC LIMIT " + limit + " OFFSET " + offset;
    }

    /**
     * 查询单个传感点的最新一条数据。
     */
    public static String selectLatestBySensor(String superTable) {
        return "SELECT ts, v FROM " + CommonUtils.quote(superTable)
                + " WHERE " + SENSOR_ID_COLUMN + " = ? AND " + DEVICE_ID_COLUMN + " = ?"
                + " ORDER BY ts DESC LIMIT 1";
    }

    /**
     * 查询设备下全部传感点的最新数据：last_row 取各子表最新一行，GROUP BY tbname 逐子表返回。
     */
    public static String selectLatestByDevice(String superTable) {
        return "SELECT last_row(ts), last_row(v), " + SENSOR_ID_COLUMN + " FROM "
                + CommonUtils.quote(superTable)
                + " WHERE " + DEVICE_ID_COLUMN + " = ? GROUP BY " + TBNAME;
    }

    /**
     * 单条写入。内联字面量只需一次往返，能显著降低单点写入延迟。
     */
    public static String insertOne(String superTable, DataPoint dataPoint) {
        return "INSERT INTO " + TdengineUtil.quoteSubTable(childTableSuffix(dataPoint))
                + " USING " + CommonUtils.quote(superTable)
                + " TAGS ('" + dataPoint.getSid() + "', '" + dataPoint.getDeviceId() + "')"
                + " VALUES (" + dataPoint.getTm() + ", " + dataPoint.getValue() + ")";
    }

    /**
     * 批量写入：每组对应同一张子表，组内多条数据合并为一条多 VALUES 的 INSERT，
     * 多个组以空格分隔拼成一条多表插入语句，一次提交。
     *
     * @param groups 按 (deviceId, sensorId) 分组后的数据点，每组非空
     */
    public static String insertMultiple(String superTable, Collection<List<DataPoint>> groups) {
        StringBuilder sql = new StringBuilder();
        for (List<DataPoint> points : groups) {
            appendInsertSegment(sql, superTable, points);
        }
        return sql.toString();
    }

    /**
     * 按 deviceId + sensorId 精确删除。
     */
    public static String deleteBySensor(String superTable) {
        return "DELETE FROM " + CommonUtils.quote(superTable)
                + " WHERE " + SENSOR_ID_COLUMN + " = ? AND " + DEVICE_ID_COLUMN + " = ?"
                + " AND ts >= ? AND ts <= ?";
    }

    /**
     * 仅按 deviceId 删除设备下全部传感点数据。
     */
    public static String deleteByDevice(String superTable) {
        return "DELETE FROM " + CommonUtils.quote(superTable)
                + " WHERE " + DEVICE_ID_COLUMN + " = ? AND ts >= ? AND ts <= ?";
    }

    /**
     * 批量删除的单条语句，sensorId 为空时按设备删除。
     */
    public static String deleteByDeviceAndSensor(String superTable, String deviceId, String sensorId,
            long startTime, long endTime) {
        StringBuilder sql = new StringBuilder("DELETE FROM ").append(CommonUtils.quote(superTable))
                .append(" WHERE ").append(DEVICE_ID_COLUMN).append(" = '").append(deviceId).append('\'');
        if (sensorId != null && !sensorId.isBlank()) {
            sql.append(" AND ").append(SENSOR_ID_COLUMN).append(" = '").append(sensorId).append('\'');
        }
        return sql.append(" AND ts >= ").append(startTime)
                .append(" AND ts <= ").append(endTime).toString();
    }

    /**
     * 追加一个「INSERT INTO 子表 USING 超级表 TAGS(...) VALUES (...),(...)」片段。
     */
    private static void appendInsertSegment(StringBuilder sql, String superTable, List<DataPoint> points) {
        DataPoint first = points.get(0);
        String sensorId = first.getSid();
        String deviceId = first.getDeviceId();
        if (sql.length() == 0) {
            sql.append("INSERT INTO ");
        } else {
            sql.append(' ');
        }
        sql.append(TdengineUtil.quoteSubTable(childTableSuffix(first)))
                .append(" USING ").append(CommonUtils.quote(superTable))
                .append(" TAGS ('").append(sensorId).append("', '").append(deviceId).append("') VALUES ");
        for (int i = 0; i < points.size(); i++) {
            if (i > 0) {
                sql.append(", ");
            }
            DataPoint point = points.get(i);
            sql.append('(').append(point.getTm()).append(", ").append(point.getValue()).append(')');
        }
    }

    /**
     * 子表名后缀：<deviceId>_<sensorId>，与 {@link TdengineUtil#quoteSubTable(String)} 拼成 s_ 前缀的完整表名。
     */
    private static String childTableSuffix(DataPoint dataPoint) {
        return dataPoint.getDeviceId() + "_" + dataPoint.getSid();
    }
}
