package com.bestdata.em.tsoss.api;

import java.util.List;
import java.util.Map;

/**
 * 时序数据库连接句柄，统一屏蔽 TDengine / IoTDB 等底层连接差异。
 */
public interface TimeSeriesClient {

    /**
     * 连接是否仍可用。
     */
    boolean isConnected();

    /**
     * 查询 deviceId 下 sensorId 在 [startTime, endTime]（epoch 毫秒，闭区间）内的全部数据点，时间倒序。
     * 当sensorId为空时，查询设备下所有传感器的数据点。
     * 
     * @param deviceId  设备 ID
     * @param sensorId  传感器 ID
     * @param startTime 起始时间（毫秒）
     * @param endTime   结束时间（毫秒）
     * @return 时间倒序的数据点列表，无数据返回空列表
     */
    List<DataPoint> query(String deviceId, String sensorId, long startTime, long endTime);

    /**
     * 查询多个传感器在 [startTime, endTime]（epoch 毫秒，闭区间）内的全部数据点，时间倒序。
     * 
     * @param dataPoints 数据点列表
     * @param startTime  起始时间（毫秒）
     * @param endTime    结束时间（毫秒）
     * @return 时间倒序的数据点列表，无数据返回空列表
     */
    List<DataPoint> query(List<DataPoint> dataPoints, long startTime, long endTime);

    /**
     * 查询最新数据。
     * 当 deviceId 与 sensorId 均非空时，精确查询该传感点的最新数据；
     * 当只有 deviceId（sensorId 为空）时，查询设备下所有传感点的最新数据。
     *
     * @param deviceId 设备 ID（必填）
     * @param sensorId 传感器 ID（可为空，为空时查询设备下全部传感点）
     * @return 最新数据点列表，无数据返回空列表
     */
    List<DataPoint> queryRealTime(String deviceId, String sensorId);

    /**
     * 查询多组传感点在 [startTime, endTime]（epoch 毫秒，闭区间）内按时间窗口聚合后的数据。
     *
     * <p>
     * {@code dataPoints} 中每个元素只需提供 {@code deviceId} 与 {@code sid}，可包含多组
     * deviceId/sensorId 组合；返回结果按 (deviceId, sensorId) 各自独立开窗聚合，
     * 每个窗口返回一个数据点，其 {@code tm} 为窗口的结束时间。
     * </p>
     *
     * @param dataPoints 待聚合的传感点列表（每个元素需提供 deviceId 与 sid）
     * @param startTime  起始时间（毫秒）
     * @param endTime    结束时间（毫秒）
     * @param window     时间窗口，格式为 "1m"、"10m"、"30m"、"1h"、"6h"、"12h"、"1d"
     * @param way        聚合方式，可选：avg, max, min, first, last
     * @return 按时间倒序的聚合数据点列表（时间为窗口结束时间），无数据返回空列表
     */
    List<DataPoint> queryAggregation(List<DataPoint> dataPoints, long startTime, long endTime, String window,
            String way);

    /**
     * 执行自定义 SQL 查询，返回原始行数据（列名 → 值），不做 DataPoint 解析。
     *
     * @author ZhenYu Chi
     * @date 2026/8/24 15:07
     */
    List<Map<String, Object>> query(String sql);

    /**
     * 执行自定义 SQL 查询，不返回结果。
     *
     * @param sql
     */
    void voidQuery(String sql);

    /**
     * 分页查询，page 从 1 开始，结果按时间倒序。
     *
     * @param sensorId  传感器 ID
     * @param startTime 起始时间（毫秒）
     * @param endTime   结束时间（毫秒）
     * @param page      页码，从 1 开始
     * @param limit     每页条数
     * @return 本页数据及总条数
     */
    PageResult<DataPoint> queryPage(String deviceId, String sensorId, long startTime, long endTime, int page,
            int limit);

    /**
     * 写入数据点，sensorId 与 deviceId 均从 {@code dataPoint} 中获取。
     *
     * @param dataPoint 数据点
     */
    void write(DataPoint dataPoint);

    /**
     * 批量写入数据点，一次请求完成；每个数据点自带 sensorId 与 deviceId。
     *
     * @param dataPoints 数据点列表
     */
    void batchWrite(List<DataPoint> dataPoints);

    /**
     * 删除 deviceId 下 sensorId 在 [startTime, endTime]（epoch 毫秒，闭区间）内的数据点。
     * 当 sensorId 为空时，删除设备下所有传感器的数据点。
     *
     * @param deviceId  设备 ID（必填）
     * @param sensorId  传感器 ID（可为空，为空时删除设备下全部传感点）
     * @param startTime 起始时间（毫秒）
     * @param endTime   结束时间（毫秒）
     */
    void delete(String deviceId, String sensorId, long startTime, long endTime);

    /**
     * 批量删除数据点，一次请求完成。
     *
     * <p>
     * {@code deleteList} 中每个元素为待删除的输出值信息：
     * </p>
     * <ul>
     * <li>{@code deviceId} —— 设备 ID</li>
     * <li>{@code id} —— 传感器 ID（可为空，为空时删除设备下全部传感点）</li>
     * <li>{@code sTm} —— 起始时间（epoch 毫秒）</li>
     * <li>{@code eTm} —— 结束时间（epoch 毫秒）</li>
     * </ul>
     *
     * @param deleteList 待删除的输出值信息列表
     */
    void batchDelete(List<Map<String, Object>> deleteList);
}
