package com.bestdata.em.tsoss.tdengine.utils;

import java.time.Instant;
import java.time.ZoneId;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 聚合窗口对齐工具。
 *
 * <p>
 * TDengine 的 INTERVAL 默认以 1970-01-01 00:00:00 UTC 为锚点切分窗口，该时刻在 UTC+8 下是
 * 本地 08:00，因此 6h、12h 这类除不尽 8 小时的窗口边界会落在 08:00 而不是 00:00。
 * 本类按指定时区算出 offset，生成 {@code 6h, 4h} 这样的窗口描述，让边界对齐到本地 0 点。
 * </p>
 */
public final class TdengineWindowResolver {

    /** 默认对齐时区，配置未指定 time-zone 时使用。 */
    public static final ZoneId DEFAULT_ZONE_ID = ZoneId.of("Asia/Shanghai");

    /** 窗口描述：正整数 + 单位（s/m/h/d），与 TdengineValidator 的校验规则保持一致。 */
    private static final Pattern WINDOW_PATTERN = Pattern.compile("^([1-9]\\d*)([smhd])$");

    private static final long MILLIS_PER_SECOND = 1_000L;
    private static final long MILLIS_PER_MINUTE = 60_000L;
    private static final long MILLIS_PER_HOUR = 3_600_000L;

    private TdengineWindowResolver() {
    }

    /**
     * 把窗口（如 {@code 6h}）转换成 INTERVAL 子句的窗口描述，必要时追加 offset 使其对齐本地 00:00。
     *
     * <p>
     * {@code d} 是自然时间单位，TDengine 本身就按本地日历 0 点切分，无需 offset；其余单位按
     * {@code offset = (-时区偏移) mod 窗口长度} 计算，结果为 0 时不追加（窗口本就已对齐）。
     * </p>
     *
     * @param window 已校验的窗口，如 1m、10m、30m、1h、6h、12h、1d
     * @param zoneId 用于对齐的时区，如 Asia/Shanghai
     * @return 可直接拼入 {@code INTERVAL(...)} 的描述，如 {@code 6h, 4h}
     */
    public static String toIntervalSpec(String window, ZoneId zoneId) {
        Matcher matcher = WINDOW_PATTERN.matcher(window);
        if (!matcher.matches()) {
            return window;
        }
        String unit = matcher.group(2);
        if ("d".equals(unit)) {
            return window;
        }
        long windowMillis = toMillis(Long.parseLong(matcher.group(1)), unit);
        long offsetMillis = resolveOffsetMillis(windowMillis, zoneId);
        if (offsetMillis <= 0) {
            return window;
        }
        return window + ", " + formatDuration(offsetMillis);
    }

    /**
     * 计算对齐本地 00:00 所需的 offset：本地 0 点相对 UTC 纪元的位移，对窗口长度取模。
     *
     * <p>
     * 取当前时刻的时区偏移量，夏令时地区在夏令时切换前后会有偏差，中国无夏令时不受影响。
     * </p>
     */
    private static long resolveOffsetMillis(long windowMillis, ZoneId zoneId) {
        int zoneOffsetSeconds = zoneId.getRules().getOffset(Instant.now()).getTotalSeconds();
        long offsetMillis = (-zoneOffsetSeconds * MILLIS_PER_SECOND) % windowMillis;
        // TDengine 不接受负数 offset，统一折算到 [0, 窗口长度) 区间
        return offsetMillis < 0 ? offsetMillis + windowMillis : offsetMillis;
    }

    /**
     * 把窗口长度换算成毫秒。
     */
    private static long toMillis(long amount, String unit) {
        return switch (unit) {
            case "s" -> amount * MILLIS_PER_SECOND;
            case "m" -> amount * MILLIS_PER_MINUTE;
            case "h" -> amount * MILLIS_PER_HOUR;
            case "d" -> amount * 24 * MILLIS_PER_HOUR;
            default -> throw new IllegalArgumentException("不支持的窗口单位: " + unit);
        };
    }

    /**
     * 把偏移量格式化成 TDengine 的时长写法，优先用较大单位（4h 优于 240m）。
     */
    private static String formatDuration(long millis) {
        if (millis % MILLIS_PER_HOUR == 0) {
            return (millis / MILLIS_PER_HOUR) + "h";
        }
        if (millis % MILLIS_PER_MINUTE == 0) {
            return (millis / MILLIS_PER_MINUTE) + "m";
        }
        return (millis / MILLIS_PER_SECOND) + "s";
    }
}
