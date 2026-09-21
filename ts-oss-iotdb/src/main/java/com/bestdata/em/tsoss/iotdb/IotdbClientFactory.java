package com.bestdata.em.tsoss.iotdb;

import com.bestdata.em.tsoss.api.DatabaseType;
import com.bestdata.em.tsoss.api.TimeSeriesClient;
import com.bestdata.em.tsoss.api.TimeSeriesClientFactory;
import com.bestdata.em.tsoss.core.ConfigUtils;
import org.apache.iotdb.session.pool.SessionPool;

import java.util.Map;

/**
 * Apache IoTDB 客户端工厂，走原生 Session API（{@link SessionPool}）。
 */
public class IotdbClientFactory implements TimeSeriesClientFactory {

    @Override
    public DatabaseType databaseType() {
        return DatabaseType.IOTDB;
    }

    /**
     * @param config 包含数据库连接配置的 Map，必须包含 "database"，
     *               可选包含 "host"、"port"、"username"、"password"、"max-pool-size"
     * @throws IllegalArgumentException 如果配置中缺少必要的 "database"
     */
    @Override
    public TimeSeriesClient create(Map<String, Object> config) {
        String host = ConfigUtils.getString(config, "host");
        String username = ConfigUtils.getString(config, "username", "user");
        String password = ConfigUtils.getString(config, "password");
        int port = ConfigUtils.getInt(config, 6667, "port");
        int maxPoolSize = ConfigUtils.getInt(config, 10, "max-pool-size", "maxPoolSize");
        String database = ConfigUtils.getString(config, "database");
        if (database == null || database.isBlank()) {
            throw new IllegalArgumentException("IoTDB 配置缺少 config.database");
        }

        SessionPool pool = new SessionPool.Builder()
                .host(host == null ? "127.0.0.1" : host)
                .port(port)
                .user(username == null ? "root" : username)
                .password(password == null ? "root" : password)
                .maxSize(maxPoolSize)
                .build();
        return new IotdbClient(pool, database);
    }
}
