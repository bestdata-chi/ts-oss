package com.bestdata.em.tsoss.api;

/**
 * 时序数据库厂商类型。
 */
public enum DatabaseType {

    /** TDengine。 */
    TDENGINE("tdengine"),

    /** Apache IoTDB。 */
    IOTDB("iotdb");

    private final String name;

    DatabaseType(String name) {
        this.name = name;
    }

    /**
     * 按名称（大小写不敏感）解析数据库类型。
     *
     * @param name 配置里的 database-name 值，例如 TDengine / IotDB / tdengine / iotdb
     * @return 对应的 {@link DatabaseType}
     * @throws IllegalArgumentException 名称为空或未知
     */
    public static DatabaseType fromName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("time-series-database.database-name 不能为空");
        }
        for (DatabaseType type : values()) {
            if (type.name.equalsIgnoreCase(name.trim())) {
                return type;
            }
        }
        throw new IllegalArgumentException(
                "未知的时序数据库类型: " + name + "（支持 tdengine / iotdb）");
    }

    public String getName() {
        return name;
    }
}
