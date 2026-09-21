package com.bestdata.em.tsoss.api;

import java.util.Map;

/**
 * 时序数据库客户端工厂 SPI。
 *
 * <p>每个数据库实现模块提供一个工厂，并在
 * {@code META-INF/services/com.bestdata.em.tsoss.api.TimeSeriesClientFactory} 中注册，
 * 由 core 的 {@code TimeSeriesClientManager} 通过 {@link java.util.ServiceLoader} 发现。</p>
 */
public interface TimeSeriesClientFactory {

    /**
     * 本工厂支持的数据库类型。
     */
    DatabaseType databaseType();

    /**
     * 根据配置创建连接。
     *
     * @param config 连接配置键值对，字段随数据库类型而不同
     * @return 建立好的连接句柄
     */
    TimeSeriesClient create(Map<String, Object> config);
}
