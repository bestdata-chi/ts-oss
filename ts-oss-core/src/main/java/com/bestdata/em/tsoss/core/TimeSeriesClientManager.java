package com.bestdata.em.tsoss.core;

import com.bestdata.em.tsoss.api.DatabaseType;
import com.bestdata.em.tsoss.api.TimeSeriesClient;
import com.bestdata.em.tsoss.api.TimeSeriesClientFactory;

import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * 时序数据库客户端管理器：按 {@link DatabaseType} 分发到对应工厂创建连接。
 */
public final class TimeSeriesClientManager {

    private final Map<DatabaseType, TimeSeriesClientFactory> factories;

    public TimeSeriesClientManager(List<TimeSeriesClientFactory> factories) {
        Map<DatabaseType, TimeSeriesClientFactory> map = new EnumMap<>(DatabaseType.class);
        for (TimeSeriesClientFactory factory : factories) {
            map.put(factory.databaseType(), factory);
        }
        this.factories = Collections.unmodifiableMap(map);
    }

    /**
     * 通过 {@link ServiceLoader} 加载 classpath 上所有注册的工厂。
     */
    public static TimeSeriesClientManager load() {
        List<TimeSeriesClientFactory> factories = ServiceLoader
                .load(TimeSeriesClientFactory.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .toList();
        return new TimeSeriesClientManager(factories);
    }

    /**
     * 创建指定数据库类型的连接。
     *
     * @throws IllegalStateException 未找到对应数据库的实现模块
     */
    public TimeSeriesClient create(DatabaseType type, Map<String, Object> config) {
        TimeSeriesClientFactory factory = factories.get(type);
        if (factory == null) {
            throw new IllegalStateException("未找到 " + type.getName() + " 的客户端实现，" + "请引入对应的 ts-oss-* 依赖");
        }
        return factory.create(config == null ? Map.of() : config);
    }
}
