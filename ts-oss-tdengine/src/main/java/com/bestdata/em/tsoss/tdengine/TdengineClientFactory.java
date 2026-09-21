package com.bestdata.em.tsoss.tdengine;

import com.bestdata.em.tsoss.api.DatabaseType;
import com.bestdata.em.tsoss.api.TimeSeriesClient;
import com.bestdata.em.tsoss.api.TimeSeriesClientFactory;
import com.bestdata.em.tsoss.core.ConfigUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

/**
 * TDengine 客户端工厂，基于 JDBC 抽象，支持三种连接方式（由 URL 前缀决定）：
 * <ul>
 *   <li>{@code jdbc:TAOS://}   —— JNI 原生（需安装 TDengine 客户端库，连 taosd 6030 端口）</li>
 *   <li>{@code jdbc:TAOS-WS://}—— WebSocket（连 taosAdapter 6041 端口，无需客户端库）</li>
 *   <li>{@code jdbc:TAOS-RS://}—— REST（连 taosAdapter 6041 端口）</li>
 * </ul>
 * URL 透传给 {@link DriverManager}，驱动按前缀自动选择，无需手动 Class.forName。
 */
public class TdengineClientFactory implements TimeSeriesClientFactory {

    /**
     * 若指定了 database，则替换 URL 中的数据库段（your_db → 实际库名），满足「数据库动态」。
     * 该方法处理 URL 字符串，确保连接到正确的数据库。
     *
     * @param url 原始数据库连接 URL
     * @param database 要连接的数据库名称
     * @return 处理后的数据库连接 URL，如果未指定数据库则返回原始 URL
     */
    static String resolveDatabase(String url, String database) {
        if (database == null || database.isBlank()) {
            return url;
        }
        int queryIdx = url.indexOf('?');
        String base = queryIdx < 0 ? url : url.substring(0, queryIdx);
        String query = queryIdx < 0 ? "" : url.substring(queryIdx);

        int schemeIdx = base.indexOf("://");
        if (schemeIdx < 0) {
            return url;
        }
        int slashIdx = base.indexOf('/', schemeIdx + 3);
        String hostPart = slashIdx < 0 ? base : base.substring(0, slashIdx);
        return hostPart + "/" + database + query;
    }

    /**
     * 从 URL 中解析数据库段（{@code jdbc:xxx://host:port/db?query} → {@code db}），取不到返回 null。
     */
    private static String extractDatabase(String url) {
        if (url == null) {
            return null;
        }
        int schemeIdx = url.indexOf("://");
        if (schemeIdx < 0) {
            return null;
        }
        int slashIdx = url.indexOf('/', schemeIdx + 3);
        if (slashIdx < 0) {
            return null;
        }
        int endIdx = url.indexOf('?', slashIdx);
        String db = endIdx < 0 ? url.substring(slashIdx + 1) : url.substring(slashIdx + 1, endIdx);
        return db.isBlank() ? null : db;
    }

    @Override
    public DatabaseType databaseType() {
        return DatabaseType.TDENGINE;
    }

    /**
     * 创建并返回一个新的 TDengine 客户端实例。
     * 从配置中获取必要的连接参数，建立连接后确保超级表 {@code <database>DataSTable} 存在。
     *
     * @param config 包含数据库连接配置的 Map，必须包含 "url" 和 "database"（或 URL 中带数据库段），
     *               可选包含 "username"、"password"
     * @return 新创建的 TimeSeriesClient 实例
     * @throws IllegalArgumentException 如果配置中缺少必要的 "url" 或 "database"
     * @throws IllegalStateException 如果数据库连接失败
     */
    @Override
    public TimeSeriesClient create(Map<String, Object> config) {
        String url = ConfigUtils.getString(config, "url");
        if (url == null) {
            throw new IllegalArgumentException("TDengine 配置缺少 config.url");
        }

        String username = ConfigUtils.getString(config, "username", "user");
        String password = ConfigUtils.getString(config, "password");
        String database = ConfigUtils.getString(config, "database");
        if (database == null || database.isBlank()) {
            database = extractDatabase(url);
        }
        if (database == null || database.isBlank()) {
            throw new IllegalArgumentException("TDengine 配置缺少 config.database（URL 中也未指定数据库）");
        }
        String superTableName = database + "DataSTable";

        String jdbcUrl = resolveDatabase(url, database);
        try {
            Connection connection = DriverManager.getConnection(
                    jdbcUrl,
                    username == null ? "root" : username,
                    password == null ? "taosdata" : password);
            // 在连接数据库的同时确保超级表存在：ts/v 为列，sensorId/deviceId 为 tag
            try (Statement st = connection.createStatement()) {
                //noinspection SqlNoDataSourceInspection
                String sql = "CREATE STABLE IF NOT EXISTS `" + superTableName + "` (ts TIMESTAMP, v FLOAT) TAGS (`sensorId` NCHAR(64), `deviceId` NCHAR(64))";
                st.execute(sql);
            }
            return new TdengineClient(connection, superTableName);
        } catch (SQLException e) {
            throw new IllegalStateException("TDengine 连接失败: " + jdbcUrl + "，原因: " + e.getMessage(), e);
        }
    }
}
