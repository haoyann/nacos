package com.alibaba.nacos.config.server.service.repository.extrnal.dialect.postgresql;


import com.alibaba.nacos.config.server.service.repository.extrnal.dialect.DialectPageHelper;

public class PostgreSQLPageHelper implements DialectPageHelper {
    
    @Override
    public String buildLimitSql(String sql, int pageNo, int pageSize) {
        final int startRow = (pageNo - 1) * pageSize;
        return sql + " limit " + pageSize + " offset " + startRow;
    }
    
    @Override
    public String replaceLimitSql(String sql, int pageNo, int pageSize) {
        return sql.replaceAll("(?i)LIMIT \\?,\\?", " offset ? limit ? ");
    }
    
}

