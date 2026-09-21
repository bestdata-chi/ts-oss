# ts-oss

时序数据库连接器：为主服务提供时序数据库的连接、查询、写入、删除能力，统一屏蔽底层引擎差异（TDengine / Apache IoTDB）。作为内部工具类 Maven 项目，上传到阿里云效制品仓库供主服务依赖。

- JDK 21，Spring Boot 3.3.x
- TDengine：基于官方 `taos-jdbcdriver`（JDBC 抽象，同时支持 JNI 原生 `jdbc:TAOS://`、WebSocket `jdbc:TAOS-WS://` 与 REST `jdbc:TAOS-RS://`）
- Apache IoTDB：基于原生 `iotdb-session`（`SessionPool`）—— 当前版本暂未启用

> **当前版本状态**：仅启用 TDengine。`ts-oss-iotdb` 模块已从聚合构建（`pom.xml` 的 `<modules>`）中排除，源码、依赖管理与 SPI 桩实现均保留，后续按需启用；下方 IoTDB 相关说明为保留的设计。

## 模块结构

```
ts-oss/
├── pom.xml                        # 聚合父 POM（版本/依赖管理）
├── ts-oss-api/                    # 稳定的公共接口与数据模型（零第三方运行时依赖）
├── ts-oss-core/                   # SPI 加载、客户端工厂管理、配置读取、校验工具
├── ts-oss-tdengine/               # TDengine 实现（taos-jdbcdriver，JDBC 抽象）
├── ts-oss-iotdb/                  # Apache IoTDB 实现（原生 Session API，当前版本未参与构建）
└── ts-oss-spring-boot-starter/    # Spring Boot 自动配置，主服务直接引入即可注入客户端
```

## 特性

- **统一 API**：`TimeSeriesClient` 屏蔽 TDengine / IoTDB 差异，切换数据库无需改业务代码。
- **SPI 可插拔**：各数据库实现通过 `META-INF/services` 注册工厂，`TimeSeriesClientManager` 用 `ServiceLoader` 自动发现。
- **多数据库支持**：`DatabaseType` 目前支持 `tdengine` 与 `iotdb`。
- **Spring Boot 自动配置**：主服务引入 starter + 一行配置即可注入 `TimeSeriesClient` Bean。
- **自动建表/建路径**：TDengine 连接时自动 `CREATE STABLE`；IoTDB 写入时按路径自动建序列。

## 快速开始（Spring Boot）

主服务在 `pom.xml` 引入 starter：

```xml
<dependency>
    <groupId>com.bestdata.em</groupId>
    <artifactId>ts-oss-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

### TDengine

```yaml
# 时序数据库连接配置
time-series-database:
    # 时序数据库厂商，可选值为 tdengine 或 iotdb
    database-name: TDengine
    config:
        # 如果使用JNI方式连接，端口为6030
        url: jdbc:TAOS-WS://10.21.135.14:6041/em6?charset=UTF-8
        username: root
        password: taosdata
        database: em6
```

### Apache IoTDB

```yaml
time-series-database:
    database-name: iotdb # 大小写不敏感
    config:
        host: 127.0.0.1
        port: 6667
        username: root
        password: root
        max-pool-size: 10 # 也可写 maxPoolSize
        database: your_database
```

启动后即可直接注入使用：

```java
@Service
public class MetricService {

    private final TimeSeriesClient client;

    public MetricService(TimeSeriesClient client) {
        this.client = client;
    }

    public void sample() {
        client.write(new DataPoint("sensor-001", "device-001", System.currentTimeMillis(), 23.5f));
        List<DataPoint> points = client.query("device-001", "sensor-001", startMs, endMs);
    }
}
```

## 编程式使用（无 Spring）

手动通过 SPI 加载并创建连接：

```java
import com.bestdata.em.tsoss.api.*;
import com.bestdata.em.tsoss.core.TimeSeriesClientManager;

Map<String, Object> config = Map.of(
    "url", "jdbc:TAOS-RS://localhost:6041/your_db",
    "username", "root",
    "password", "taosdata"
);
TimeSeriesClientManager manager = TimeSeriesClientManager.load();
try (TimeSeriesClient client = manager.create(DatabaseType.TDENGINE, config)) {
    client.write(new DataPoint("sensor-001", "device-001", System.currentTimeMillis(), 23.5f));
    List<DataPoint> points = client.query("device-001", "sensor-001", startMs, endMs);
}
```

> `TimeSeriesClient` 同时实现了 `AutoCloseable`，使用完应调用 `close()` 释放底层连接。

## API 一览

核心接口 [`TimeSeriesClient`](ts-oss-api/src/main/java/com/bestdata/em/tsoss/api/TimeSeriesClient.java)：

| 方法 | 说明 |
| --- | --- |
| `isConnected()` | 连接是否仍可用 |
| `query(String deviceId, String sensorId, long startTime, long endTime)` | 查询设备下传感点在 `[startTime, endTime]`（epoch 毫秒，闭区间）内的数据点，时间倒序；sensorId 为空时查询设备下全部传感点 |
| `query(List<DataPoint> dataPoints, long startTime, long endTime)` | 批量精确查询多个传感点（从 dataPoints 取 deviceId/sid），一次往返，时间倒序 |
| `query(String sql)` | 执行自定义 SQL，返回原始行数据（`List<Map<String, Object>>`，列名 → 值），不做 `DataPoint` 解析 |
| `voidQuery(String sql)` | 执行自定义 SQL，不返回结果 |
| `queryPage(String deviceId, String sensorId, long startTime, long endTime, int page, int limit)` | 分页精确查询，`page` 从 1 开始，返回本页数据及总条数 |
| `queryRealTime(String deviceId, String sensorId)` | 查询最新数据：sensorId 非空时精确查询该传感点最新一条；sensorId 为空时查询设备下全部传感点最新数据 |
| `write(DataPoint dataPoint)` | 写入单个数据点（sensorId/deviceId 取自 dataPoint） |
| `batchWrite(List<DataPoint> dataPoints)` | 批量写入数据点（每个数据点自带 sensorId/deviceId） |
| `delete(String deviceId, String sensorId, long startTime, long endTime)` | 删除设备下 `[startTime, endTime]` 内的数据点（sensorId 为空时删除设备下全部传感点） |
| `batchDelete(List<Map<String, Object>> deleteList)` | 批量删除（每条含 `deviceId`/`id`/`sTm`/`eTm`，`id` 为空时删除该设备下全部传感点） |

### 数据模型

- [`DataPoint`](ts-oss-api/src/main/java/com/bestdata/em/tsoss/api/DataPoint.java)：`tm`（epoch 毫秒）+ `value`（`float`）+ `sid`（传感器 ID）+ `deviceId`（设备 ID）+ `id`（默认为时间戳字符串）；写入与精确查询时需携带 `deviceId` 与 `sid`。
- [`PageResult<T>`](ts-oss-api/src/main/java/com/bestdata/em/tsoss/api/PageResult.java)：`data`（本页数据）、`total`（总条数）、`page`、`limit`。

## 底层数据模型

### TDengine

- 超级表：`<database>DataSTable`，列为 `ts TIMESTAMP`、`v FLOAT`，标签为 `sensorId NCHAR(64)`、`deviceId NCHAR(64)`；连接时自动创建。
- 子表：`s_<deviceId>_<sensorId>`，通过 `USING` 关联超级表，标签值为 `sensorId`、`deviceId`。
- 写入采用内联 `INSERT ... VALUES`，批量写合并为单条多 `VALUES` 语句，单次网络往返。
- URL 前缀决定连接方式：

| 前缀              | 方式      | 说明                                         |
| ----------------- | --------- | -------------------------------------------- |
| `jdbc:TAOS://`    | JNI 原生  | 需安装 TDengine 客户端库，连 taosd 6030 端口 |
| `jdbc:TAOS-WS://` | WebSocket | 连 taosAdapter 6041 端口，无需客户端库       |
| `jdbc:TAOS-RS://` | REST      | 连 taosAdapter 6041 端口                     |

### Apache IoTDB

- 时间序列路径：`root.<database>.<deviceId>.<sensorId>`，`deviceId` 作为设备节点（设备维度天然索引）。
- 基于 `SessionPool`，写入走 `insertRecord`（`FLOAT` 类型测点）。
- 按 `sensorId` 查询/删除时用通配路径 `root.<database>.*.<sensorId>` 匹配设备节点（假设一个 sensor 只属于一个 device）。

## 配置参考

### TDengine（`config.*`）

| 键         | 必填 | 默认值      | 说明                                  |
| ---------- | ---- | ----------- | ------------------------------------- |
| `url`      | 是   | —           | JDBC URL，前缀决定 JNI / WS / RS 方式 |
| `database` | 否   | 从 URL 解析 | 数据库名；指定后会替换 URL 中的库名段 |
| `username` | 否   | `root`      | 用户名（也支持 `user` 键）            |
| `password` | 否   | `taosdata`  | 密码                                  |

### IoTDB（`config.*`）

| 键              | 必填 | 默认值      | 说明                                  |
| --------------- | ---- | ----------- | ------------------------------------- |
| `database`      | 是   | —           | 数据库名                              |
| `host`          | 否   | `127.0.0.1` | 服务地址                              |
| `port`          | 否   | `6667`      | 端口                                  |
| `username`      | 否   | `root`      | 用户名（也支持 `user` 键）            |
| `password`      | 否   | `root`      | 密码                                  |
| `max-pool-size` | 否   | `10`        | 连接池大小（也支持 `maxPoolSize` 键） |

> `ConfigUtils` 会屏蔽 key 写法差异：字符串/整数配置均支持多个别名 key 依次尝试。

## 构建

```bash
mvn clean install
```

## 发布到云效制品仓库

发布走 settings.xml 里激活 profile 的 `altReleaseDeploymentRepository` / `altSnapshotDeploymentRepository` 属性，**POM 里无需 `distributionManagement`**。

### 1. 配置 server 凭据

在 settings.xml（全局 `$MAVEN_HOME/conf/settings.xml` 或用户级 `~/.m2/settings.xml`）里配置 `<server>`，`<id>` 需与部署仓库 id 一致（本项目为 `repo-absln`）：

```xml
<servers>
  <server>
    <id>repo-absln</id>
    <username>${云效用户名}</username>
    <password>${云效访问令牌/AccessToken}</password>
  </server>
</servers>
```

### 2. 配置部署仓库（激活 profile）

仓库地址在云效控制台 → 制品仓库 → 获取仓库地址 复制：

```xml
<profiles>
  <profile>
    <id>rdc</id>
    <properties>
      <altReleaseDeploymentRepository>repo-absln::https://packages.aliyun.com/6178beb110204867ecfd6872/maven/repo-absln</altReleaseDeploymentRepository>
      <altSnapshotDeploymentRepository>repo-absln::https://packages.aliyun.com/6178beb110204867ecfd6872/maven/repo-absln</altSnapshotDeploymentRepository>
    </properties>
  </profile>
</profiles>
<activeProfiles>
  <activeProfile>rdc</activeProfile>
</activeProfiles>
```

> 部署仓库的 `repo-absln` 要与第 1 步 `<server>` 的 id 一一对应。用 `id::url` 两段式写法；若写成 `id::default::url` 三段式，deploy 时会提示 legacy syntax 警告（不影响上传）。

### 3. 发布

```bash
mvn clean deploy
```

发布坐标：`com.bestdata.em:ts-oss-*:1.0.0`
