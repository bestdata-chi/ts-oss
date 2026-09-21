package com.bestdata.em.tsoss.api;

import lombok.Getter;

import java.util.List;

/**
 * 分页查询结果。
 *
 * @param <T> 数据项类型
 */
@Getter
public final class PageResult<T> {

    /** 本页数据。 */
    private final List<T> data;

    /** 满足条件的总条数。 */
    private final long total;

    /** 当前页，从 1 开始。 */
    private final int page;

    /** 每页条数。 */
    private final int limit;

    public PageResult(List<T> data, long total, int page, int limit) {
        this.data = data;
        this.total = total;
        this.page = page;
        this.limit = limit;
    }
}
