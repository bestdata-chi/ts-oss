package com.bestdata.em.tsoss.tdengine.utils;

import com.bestdata.em.tsoss.api.DataPoint;
import com.bestdata.em.tsoss.core.CommonUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class TdengineUtil {

    /** 窗口边界：窗口开始/结束时间，作为聚合结果的时间列使用。 */
    public static final String WINDOW_END_EXPR = "_wend";
    public static final String WINDOW_START_EXPR = "_wstart";

    /** COLS 函数的最低服务端版本，低于该版本无法取回极值所在记录的时间。 */
    private static final int[] COLS_MIN_VERSION = { 3, 3, 6, 0 };

    /**
     * 判断版本号是否不低于 {@code minVersion}，逐段比较数字，缺失的段按 0 处理。
     */
    public static boolean isVersionAtLeast(String version) {
        if (version == null || version.isBlank()) {
            return false;
        }
        int[] minVersion = COLS_MIN_VERSION;

        String[] parts = version.trim().split("\\.");
        for (int i = 0; i < minVersion.length; i++) {
            int actual = i < parts.length ? parseLeadingInt(parts[i]) : 0;
            if (actual != minVersion[i]) {
                return actual > minVersion[i];
            }
        }
        return true;
    }

    /** 取版本段的前导数字，无法解析时按 0 处理（如 "8-dev" 取 8）。 */
    private static int parseLeadingInt(String part) {
        int end = 0;
        while (end < part.length() && Character.isDigit(part.charAt(end))) {
            end++;
        }
        return end == 0 ? 0 : Integer.parseInt(part.substring(0, end));
    }

    /** 是否为取极值的聚合方式（max/min），仅这两者需要 COLS 才能拿到极值所在记录的时间。 */
    public static boolean isExtremum(String aggregation) {
        return "max".equals(aggregation) || "min".equals(aggregation);
    }

    /**
     * 对子表标识符进行 SQL 转义，添加反引号。
     *
     * @param identifier 子表标识符
     * @return
     */
    public static String quoteSubTable(String identifier) {
        CommonUtils.requireNotBlank(identifier, "identifier");
        return CommonUtils.quote("s_" + identifier);
    }

    /**
     * 数据封装 - 多输出值查询
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
     * 数据封装 - 单输出值查询
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
            CommonUtils.requireNotBlank(deviceId, "deviceId");
            CommonUtils.requireNotBlank(sensorId, "sensorId");
            tableNames.add("s_" + deviceId + "_" + sensorId);
        }
        return new ArrayList<>(tableNames);
    }


}
