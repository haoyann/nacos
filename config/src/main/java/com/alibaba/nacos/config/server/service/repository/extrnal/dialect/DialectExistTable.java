package com.alibaba.nacos.config.server.service.repository.extrnal.dialect;


public interface DialectExistTable {
    
    /**
     * Determine whether the table exists.
     *
     * @param tableName table name
     * @return {@code true} check exist sql
     */
    String isExistTable(String tableName);
}
