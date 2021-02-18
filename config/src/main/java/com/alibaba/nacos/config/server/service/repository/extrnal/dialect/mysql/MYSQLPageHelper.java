package com.alibaba.nacos.config.server.service.repository.extrnal.dialect.mysql;


import com.alibaba.nacos.config.server.service.repository.extrnal.dialect.DialectPageHelper;

public class MYSQLPageHelper implements DialectPageHelper {
    
    @Override
    public String buildLimitSql(String sql, int pageNo, int pageSize) {
        final int startRow = (pageNo - 1) * pageSize;
        return sql + " limit " + startRow + " , " + pageSize;
    }
    
    @Override
    public String replaceLimitSql(String sql, int pageNo, int pageSize) {
        return sql;
    }
    
}
