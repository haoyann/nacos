/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.jdbc;

import com.alibaba.nacos.config.server.model.ConfigInfoAggr;
import com.alibaba.nacos.config.server.model.ConfigInfoChanged;
import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.service.repository.PaginationHelper;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.ConfigInfoAggrRepository;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.TransactionCallback;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import static com.alibaba.nacos.config.server.service.repository.RowMapperManager.CONFIG_INFO_AGGR_ROW_MAPPER;
import static com.alibaba.nacos.config.server.service.repository.RowMapperManager.CONFIG_INFO_CHANGED_ROW_MAPPER;

public class JDBCConfigInfoAggrRepository extends BaseJDBCRepository implements ConfigInfoAggrRepository {
    
    @Override
    public boolean addAggrConfigInfo(String dataId, String group, String tenant, String datumId, String appName,
            String content) {
        String appNameTmp = StringUtils.isBlank(appName) ? StringUtils.EMPTY : appName;
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        final Timestamp now = new Timestamp(System.currentTimeMillis());
        String select = "SELECT content FROM config_info_aggr WHERE data_id = ? AND group_id = ? AND tenant_id = ?  AND datum_id = ?";
        String insert = "INSERT INTO config_info_aggr(data_id, group_id, tenant_id, datum_id, app_name, content, gmt_modified) VALUES(?,?,?,?,?,?,?) ";
        String update = "UPDATE config_info_aggr SET content = ? , gmt_modified = ? WHERE data_id = ? AND group_id = ? AND tenant_id = ? AND datum_id = ?";
        
        try {
            try {
                String dbContent = jt
                        .queryForObject(select, new Object[] {dataId, group, tenantTmp, datumId}, String.class);
                
                if (dbContent != null && dbContent.equals(content)) {
                    return true;
                } else {
                    return jt.update(update, content, now, dataId, group, tenantTmp, datumId) > 0;
                }
            } catch (EmptyResultDataAccessException ex) { // no data, insert
                return jt.update(insert, dataId, group, tenantTmp, datumId, appNameTmp, content, now) > 0;
            }
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public void removeSingleAggrConfigInfo(String dataId, String group, String tenant, String datumId) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        String sql = "DELETE FROM config_info_aggr WHERE data_id=? AND group_id=? AND tenant_id=? AND datum_id=?";
        
        try {
            this.jt.update(sql, new PreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps) throws SQLException {
                    int index = 1;
                    ps.setString(index++, dataId);
                    ps.setString(index++, group);
                    ps.setString(index++, tenantTmp);
                    ps.setString(index, datumId);
                }
            });
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public void removeAggrConfigInfo(String dataId, String group, String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        String sql = "DELETE FROM config_info_aggr WHERE data_id=? AND group_id=? AND tenant_id=?";
        
        try {
            this.jt.update(sql, new PreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps) throws SQLException {
                    int index = 1;
                    ps.setString(index++, dataId);
                    ps.setString(index++, group);
                    ps.setString(index, tenantTmp);
                }
            });
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public boolean batchRemoveAggr(String dataId, String group, String tenant, List<String> datumList) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        final StringBuilder datumString = new StringBuilder();
        for (String datum : datumList) {
            datumString.append("'").append(datum).append("',");
        }
        datumString.deleteCharAt(datumString.length() - 1);
        final String sql =
                "delete from config_info_aggr where data_id=? and group_id=? and tenant_id=? and datum_id in ("
                        + datumString.toString() + ")";
        try {
            jt.update(sql, dataId, group, tenantTmp);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            return false;
        }
        return true;
    }
    
    @Override
    public boolean replaceAggr(String dataId, String group, String tenant, Map<String, String> datumMap,
            String appName) {
        try {
            Boolean isReplaceOk = tjt.execute(new TransactionCallback<Boolean>() {
                @Override
                public Boolean doInTransaction(TransactionStatus status) {
                    try {
                        String appNameTmp = appName == null ? "" : appName;
                        removeAggrConfigInfo(dataId, group, tenant);
                        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
                        String sql = "INSERT INTO config_info_aggr(data_id, group_id, tenant_id, datum_id, app_name, content, gmt_modified) VALUES(?,?,?,?,?,?,?) ";
                        for (Map.Entry<String, String> datumEntry : datumMap.entrySet()) {
                            jt.update(sql, dataId, group, tenantTmp, datumEntry.getKey(), appNameTmp,
                                    datumEntry.getValue(), new Timestamp(System.currentTimeMillis()));
                        }
                    } catch (Throwable e) {
                        throw new TransactionSystemException("error in addAggrConfigInfo");
                    }
                    return Boolean.TRUE;
                }
            });
            if (isReplaceOk == null) {
                return false;
            }
            return isReplaceOk;
        } catch (TransactionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            return false;
        }
    }
    
    @Override
    public int aggrConfigInfoCount(String dataId, String group, String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        String sql = " SELECT COUNT(ID) FROM config_info_aggr WHERE data_id = ? AND group_id = ? AND tenant_id = ?";
        Integer result = jt.queryForObject(sql, Integer.class, new Object[] {dataId, group, tenantTmp});
        if (result == null) {
            throw new IllegalArgumentException("aggrConfigInfoCount error");
        }
        return result.intValue();
    }
    
    @Override
    public int aggrConfigInfoCount(String dataId, String group, String tenant, List<String> datumIds, boolean isIn) {
        if (datumIds == null || datumIds.isEmpty()) {
            return 0;
        }
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        StringBuilder sql = new StringBuilder(
                " SELECT COUNT(*) FROM config_info_aggr WHERE data_id = ? and group_id = ? and tenant_id = ? and datum_id");
        if (isIn) {
            sql.append(" in (");
        } else {
            sql.append(" not in (");
        }
        for (int i = 0, size = datumIds.size(); i < size; i++) {
            if (i > 0) {
                sql.append(", ");
            }
            sql.append("?");
        }
        sql.append(")");
        
        List<Object> objectList = Lists.<Object>newArrayList(dataId, group, tenantTmp);
        objectList.addAll(datumIds);
        Integer result = jt.queryForObject(sql.toString(), Integer.class, objectList.toArray());
        if (result == null) {
            throw new IllegalArgumentException("aggrConfigInfoCount error");
        }
        return result.intValue();
    }
    
    @Override
    public Page<ConfigInfoAggr> findConfigInfoAggrByPage(String dataId, String group, String tenant, final int pageNo,
            final int pageSize) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        String sqlCountRows = "SELECT COUNT(*) FROM config_info_aggr WHERE data_id = ? and group_id = ? and tenant_id = ?";
        String sqlFetchRows =
                "select data_id,group_id,tenant_id,datum_id,app_name,content from config_info_aggr where data_id=? and "
                        + "group_id=? and tenant_id=? order by datum_id limit ?,?";
        PaginationHelper<ConfigInfoAggr> helper = createPaginationHelper();
        try {
            return helper.fetchPageLimit(sqlCountRows, new Object[] {dataId, group, tenantTmp}, sqlFetchRows,
                    new Object[] {dataId, group, tenantTmp, (pageNo - 1) * pageSize, pageSize}, pageNo, pageSize,
                    CONFIG_INFO_AGGR_ROW_MAPPER);
            
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public List<ConfigInfoChanged> findAllAggrGroup() {
        String sql = "SELECT DISTINCT data_id, group_id, tenant_id FROM config_info_aggr";
        
        try {
            return jt.query(sql, new Object[] {}, CONFIG_INFO_CHANGED_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (Exception e) {
            LogUtil.FATAL_LOG.error("[db-other-error]" + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
