package com.bestdata.em.tsoss.core;

import com.bestdata.em.tsoss.api.DataPoint;

/**
 * 各数据库实现共用的校验工具。
 */
public final class CommonUtils {

    private CommonUtils() {
    }

    /**
     * 校验字符串非空，null 或空白即抛异常。
     */
    public static void requireNotBlank(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " 不能为空");
        }
    }

    /**
     * 校验数据点有效：非 null，且测值为有限浮点数（非 NaN/Infinity）。
     */
    public static void requireDataPoint(DataPoint dataPoint) {
        if (dataPoint == null) {
            throw new IllegalArgumentException("dataPoint 不能为空");
        }
        if (Float.isNaN(dataPoint.getValue()) || Float.isInfinite(dataPoint.getValue())) {
            throw new IllegalArgumentException("dataPoint 测值不能为 NaN 或 Infinity");
        }
    }

    /**
     * 对标识符进行 SQL 转义，添加反引号。
     * @param identifier
     * @return
     */
    public static String quote(String identifier) {
        return "`" + identifier + "`";
    }
}
