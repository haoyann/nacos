package com.alibaba.nacos.config.server.service.repository.extrnal.dialect.mysql;


import com.alibaba.nacos.config.server.service.repository.extrnal.dialect.DialectExistTable;

public class MYSQLExistTable implements DialectExistTable {
    
    @Override
    public String isExistTable(String tableName) {
        return String.format("select 1 from %s limit 1", tableName);
    }
}
