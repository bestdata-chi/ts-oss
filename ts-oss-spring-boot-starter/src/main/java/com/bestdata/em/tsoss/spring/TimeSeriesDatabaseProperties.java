package com.bestdata.em.tsoss.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;

/**
 * 主服务 {@code time-series-database} 配置绑定。
 */
@ConfigurationProperties(prefix = "time-series-database")
public class TimeSeriesDatabaseProperties {

    /**
     * 数据库厂商，可选 tdengine / iotdb（大小写不敏感）。
     */
    private String databaseName;

    /**
     * 各数据库私有的连接配置（url / host / port / username / password / ...）。
     */
    private Map<String, Object> config = new HashMap<>();

    public String getDatabaseName() {
        return databaseName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public void setConfig(Map<String, Object> config) {
        this.config = config;
    }
}
