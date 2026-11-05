package com.bestdata.em.tsoss.tdengine.utils;

import com.bestdata.em.tsoss.api.DataPoint;
import com.bestdata.em.tsoss.core.CommonUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * TDengine 通用工具：结果集映射、参数绑定、子表名收集与版本判断。
 *
 * <p>
 * 本类只做与具体查询语义无关的通用处理；SQL 文本拼装见 {@link TdengineSqlBuilder}，
 * 入参校验见 {@link TdengineValidator}。
 * </p>
 */
public final class TdengineUtil {

    /** 窗口开始时间伪列，作为聚合结果的时间列使用。 */
    public static final String WINDOW_START_EXPR = "_wstart";

    /** 窗口结束时间伪列。 */
    public static final String WINDOW_END_EXPR = "_wend";

    /** COLS 函数的最低服务端版本，低于该版本无法取回极值所在记录的时间。 */
    private static final int[] COLS_MIN_VERSION = { 3, 3, 6, 0 };

    private TdengineUtil() {
    }

    /**
     * 数据封装 - 多输出值查询，结果集需包含 sensorId、ts、v 三列。
     *
     * @author ZhenYu Chi
     * @date 2026/8/25 09:07
     */
    public static List<DataPoint> getDataPoints(PreparedStatement ps) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            List<DataPoint> result = new ArrayList<>();
            while (rs.next()) {
                result.add(new DataPoint(rs.getString("sensorId"),
                        rs.getTimestamp("ts").getTime(), rs.getFloat("v")));
            }
            return result;
        }
    }

    /**
     * 数据封装 - 单输出值查询，sensorId 由调用方给出。
     *
     * @author ZhenYu Chi
     * @date 2026/8/25 09:07
     */
    public static List<DataPoint> getDataPoints(PreparedStatement ps, String sensorId) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            List<DataPoint> result = new ArrayList<>();
            while (rs.next()) {
                result.add(new DataPoint(sensorId, rs.getTimestamp("ts").getTime(), rs.getFloat("v")));
            }
            return result;
        }
    }

    /**
     * 数据封装 - 多设备查询，结果集需包含 sensorId、deviceId、ts、v 四列。
     */
    public static List<DataPoint> getDeviceDataPoints(PreparedStatement ps) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            List<DataPoint> result = new ArrayList<>();
            while (rs.next()) {
                result.add(new DataPoint(rs.getString("sensorId"), rs.getString("deviceId"),
                        rs.getTimestamp("ts").getTime(), rs.getFloat("v")));
            }
            return result;
        }
    }

    /**
     * 数据封装 - 聚合查询，时间列为 {@link TdengineSqlBuilder#TIME_COLUMN_ALIAS}，空值行跳过。
     */
    public static List<DataPoint> getAggregationDataPoints(PreparedStatement ps) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            List<DataPoint> result = new ArrayList<>();
            while (rs.next()) {
                Timestamp tm = rs.getTimestamp(TdengineSqlBuilder.TIME_COLUMN_ALIAS);
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

    /**
     * 依次绑定子表名参数。
     *
     * @return 下一个可用的参数下标
     */
    public static int bindTableNames(PreparedStatement ps, List<String> tableNames, int startIndex)
            throws SQLException {
        int index = startIndex;
        for (String tableName : tableNames) {
            ps.setString(index++, tableName);
        }
        return index;
    }

    /**
     * 绑定 [startTime, endTime] 时间区间参数。
     */
    public static void bindTimeRange(PreparedStatement ps, int startIndex, long startTime, long endTime)
            throws SQLException {
        int index = startIndex;
        ps.setTimestamp(index++, new Timestamp(startTime));
        ps.setTimestamp(index, new Timestamp(endTime));
    }

    /**
     * 按固定大小切分列表，用于分批查询，返回的每个子列表非空。
     */
    public static <T> List<List<T>> partition(List<T> source, int size) {
        if (source == null || source.isEmpty() || size <= 0) {
            return Collections.emptyList();
        }
        List<List<T>> partitions = new ArrayList<>((source.size() + size - 1) / size);
        for (int from = 0; from < source.size(); from += size) {
            partitions.add(source.subList(from, Math.min(from + size, source.size())));
        }
        return partitions;
    }

    /**
     * 校验每个数据点的 deviceId/sensorId，并收集去重后的子表名，保持首次出现的顺序。
     */
    public static List<String> collectTableNames(List<DataPoint> dataPoints) {
        Set<String> tableNames = new LinkedHashSet<>();
        for (DataPoint dataPoint : dataPoints) {
            if (dataPoint == null) {
                throw new IllegalArgumentException("dataPoints 中存在空元素");
            }
            String deviceId = dataPoint.getDeviceId();
            String sensorId = dataPoint.getSid();
            TdengineValidator.requireDeviceId(deviceId);
            TdengineValidator.requireSensorId(sensorId);
            tableNames.add("s_" + deviceId + "_" + sensorId);
        }
        return new ArrayList<>(tableNames);
    }

    /**
     * 把聚合结果值统一转成 float。
     *
     * <p>
     * 聚合结果的实际类型随函数而变：avg 返回 DOUBLE（驱动里是 Double），first/last/max/min
     * 返回 FLOAT（Float）。taos-jdbcdriver 的 getFloat/getDouble 会以
     * {@code BigDecimal.compareTo(MIN_VALUE)} 做范围校验，值为 0 或负数时误判越界并抛
     * {@code 0x2314 ... outside valid range for the jdbcType}，因此统一走不做校验的 getObject
     * 再按 Number 自行收窄。
     * </p>
     */
    public static float toFloat(Object raw) {
        if (raw instanceof Number number) {
            return number.floatValue();
        }
        return Float.parseFloat(raw.toString());
    }

    /**
     * 对子表标识符进行 SQL 转义，添加反引号。
     *
     * @param identifier 子表标识符，不含 s_ 前缀
     */
    public static String quoteSubTable(String identifier) {
        CommonUtils.requireNotBlank(identifier, "identifier");
        return CommonUtils.quote("s_" + identifier);
    }

    /**
     * 判断版本号是否不低于 COLS 函数要求的最低版本，逐段比较数字，缺失的段按 0 处理。
     */
    public static boolean isVersionAtLeast(String version) {
        if (version == null || version.isBlank()) {
            return false;
        }
        String[] parts = version.trim().split("\\.");
        for (int i = 0; i < COLS_MIN_VERSION.length; i++) {
            int actual = i < parts.length ? parseLeadingInt(parts[i]) : 0;
            if (actual != COLS_MIN_VERSION[i]) {
                return actual > COLS_MIN_VERSION[i];
            }
        }
        return true;
    }

    /**
     * 是否为取极值的聚合方式（max/min），仅这两者需要 COLS 才能拿到极值所在记录的时间。
     */
    public static boolean isExtremum(String aggregation) {
        return "max".equals(aggregation) || "min".equals(aggregation);
    }

    /**
     * 取版本段的前导数字，无法解析时按 0 处理（如 "8-dev" 取 8）。
     */
    private static int parseLeadingInt(String part) {
        int end = 0;
        while (end < part.length() && Character.isDigit(part.charAt(end))) {
            end++;
        }
        return end == 0 ? 0 : Integer.parseInt(part.substring(0, end));
    }
}
