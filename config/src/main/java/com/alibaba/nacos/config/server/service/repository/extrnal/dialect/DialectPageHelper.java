package com.alibaba.nacos.config.server.service.repository.extrnal.dialect;


public interface DialectPageHelper {
    
    /**
     * build limit sql
     *
     * @param sql      original sql
     * @param pageNo   pageNo start from 1
     * @param pageSize pageSize
     * @return limit sql
     */
    String buildLimitSql(String sql, int pageNo, int pageSize);
    
    /**
     * replace original limit sql
     *
     * @param sql      original sql 'limit ?,?'
     * @param pageNo   pageNo start from 1
     * @param pageSize pageSize
     * @return new limit sql
     */
    String replaceLimitSql(String sql, int pageNo, int pageSize);
    
}
