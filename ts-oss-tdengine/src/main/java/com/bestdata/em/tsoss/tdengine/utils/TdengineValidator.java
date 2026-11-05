package com.bestdata.em.tsoss.tdengine.utils;

import com.bestdata.em.tsoss.api.DataPoint;
import com.bestdata.em.tsoss.core.CommonUtils;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * TDengine 入参校验工具，集中收敛各方法开头重复的参数判断。
 *
 * <p>
 * 校验失败统一抛 {@link IllegalArgumentException}，异常信息带参数名，便于定位调用方问题。
 * </p>
 */
public final class TdengineValidator {

    /** 支持的聚合方式，作为白名单校验后再拼入 SQL，避免注入。 */
    private static final Set<String> AGGREGATION_WAYS = Set.of("avg", "max", "min", "first", "last");

    /** 窗口格式：正整数 + 单位（s/m/h/d），如 1m、10m、6h、1d。 */
    private static final Pattern WINDOW_PATTERN = Pattern.compile("^[1-9]\\d*[smhd]$");

    /** 聚合方式白名单提示语。 */
    private static final String AGGREGATION_HINT = "way 仅支持 avg、max、min、first、last: ";

    private TdengineValidator() {
    }

    /**
     * 判断字符串是否有实际内容，用于「可选参数」的分支判断。
     */
    public static boolean isNotBlank(String value) {
        return value != null && !value.isBlank();
    }

    /**
     * 校验设备 ID 非空。
     */
    public static void requireDeviceId(String deviceId) {
        CommonUtils.requireNotBlank(deviceId, "deviceId");
    }

    /**
     * 校验传感点 ID 非空。
     */
    public static void requireSensorId(String sensorId) {
        CommonUtils.requireNotBlank(sensorId, "sensorId");
    }

    /**
     * 校验时间区间合法：起始时间不能晚于结束时间。
     */
    public static void requireTimeRange(long startTime, long endTime) {
        if (startTime > endTime) {
            throw new IllegalArgumentException("startTime 不能大于 endTime");
        }
    }

    /**
     * 校验批量删除项的时间区间，字段名沿用入参约定。
     */
    public static void requireDeleteTimeRange(long startTime, long endTime) {
        if (startTime > endTime) {
            throw new IllegalArgumentException("sTm 不能大于 eTm!");
        }
    }

    /**
     * 校验分页参数：页码从 1 开始，每页条数必须为正。
     */
    public static void requirePage(int page, int limit) {
        if (page < 1) {
            throw new IllegalArgumentException("page 必须从 1 开始");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("limit 必须大于 0");
        }
    }

    /**
     * 校验传感点列表非空。
     */
    public static void requireDataPoints(List<DataPoint> dataPoints) {
        if (dataPoints == null || dataPoints.isEmpty()) {
            throw new IllegalArgumentException("dataPoints 不能为空");
        }
    }

    /**
     * 校验聚合窗口格式，如 1m、10m、6h、1d。
     */
    public static void requireWindow(String window) {
        if (window == null || !WINDOW_PATTERN.matcher(window).matches()) {
            throw new IllegalArgumentException(
                    "window 格式非法，应为 \"1m\"、\"10m\"、\"30m\"、\"1h\"、\"6h\"、\"12h\"、\"1d\" 等: " + window);
        }
    }

    /**
     * 校验并归一化聚合方式，返回可直接拼入 SQL 的小写形式。
     *
     * @param way 聚合方式，大小写不敏感
     * @return 归一化后的聚合方式
     */
    public static String requireAggregation(String way) {
        String aggregation = way == null ? null : way.toLowerCase(Locale.ROOT);
        if (aggregation == null || !AGGREGATION_WAYS.contains(aggregation)) {
            throw new IllegalArgumentException(AGGREGATION_HINT + way);
        }
        return aggregation;
    }
}
