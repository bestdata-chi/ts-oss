package com.bestdata.em.tsoss.spring;

import com.bestdata.em.tsoss.api.DatabaseType;
import com.bestdata.em.tsoss.api.TimeSeriesClient;
import com.bestdata.em.tsoss.core.TimeSeriesClientManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * 时序数据库自动配置：根据 database-name 动态创建连接 bean。
 */
@AutoConfiguration
@EnableConfigurationProperties(TimeSeriesDatabaseProperties.class)
@ConditionalOnProperty(prefix = "time-series-database", name = "database-name")
public class TimeSeriesAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(TimeSeriesAutoConfiguration.class);

    /**
     * 创建并配置一个TimeSeriesClient Bean
     * 当容器中不存在该类型的Bean时才会创建
     * 当Bean销毁时会自动调用close方法进行资源释放
     *
     * @param properties 时间序列数据库的配置属性
     * @return 配置好的TimeSeriesClient实例
     */
    @Bean(destroyMethod = "close") // 指定Bean销毁时调用的方法为close
    @ConditionalOnMissingBean // 当容器中不存在相同类型的Bean时才创建此Bean
    public TimeSeriesClient timeSeriesClient(TimeSeriesDatabaseProperties properties) {
        // 根据配置的数据库类型名称获取对应的数据库类型枚举
        DatabaseType type = DatabaseType.fromName(properties.getDatabaseName());
        // 使用TimeSeriesClientManager创建指定类型和配置的客户端实例
        TimeSeriesClient client = TimeSeriesClientManager.load().create(type, properties.getConfig());
        log.info("Time series database connection successful: {}", type.getName());
        return client;
    }
}
