package com.bestdata.em.tsoss.core;

import java.util.Map;

/**
 * 配置键值对读取工具，屏蔽 key 写法差异（如 max-pool-size / maxPoolSize）。
 */
public final class ConfigUtils {

    private ConfigUtils() {
    }

    /**
     * 依次尝试多个 key，返回第一个非空字符串值；都取不到返回 {@code null}。
     */
    public static String getString(Map<String, Object> config, String... keys) {
        for (String key : keys) {
            Object value = config.get(key);
            if (value == null) {
                continue;
            }
            String text = String.valueOf(value).trim();
            if (!text.isEmpty()) {
                return text;
            }
        }
        return null;
    }

    /**
     * 依次尝试多个 key，返回第一个可解析的整数值；都取不到返回默认值。
     */
    public static int getInt(Map<String, Object> config, int defaultValue, String... keys) {
        for (String key : keys) {
            Object value = config.get(key);
            if (value instanceof Number number) {
                return number.intValue();
            }
            if (value instanceof String text && !text.isBlank()) {
                try {
                    return Integer.parseInt(text.trim());
                } catch (NumberFormatException ignored) {
                    // 尝试下一个 key
                }
            }
        }
        return defaultValue;
    }

    /**
     * 依次尝试多个 key，返回第一个可解析的长整数值；都取不到返回默认值。
     */
    public static long getLong(Map<String, Object> config, long defaultValue, String... keys) {
        for (String key : keys) {
            Object value = config.get(key);
            if (value instanceof Number number) {
                return number.longValue();
            }
            if (value instanceof String text && !text.isBlank()) {
                try {
                    return Long.parseLong(text.trim());
                } catch (NumberFormatException ignored) {
                    // 尝试下一个 key
                }
            }
        }
        return defaultValue;
    }
}
