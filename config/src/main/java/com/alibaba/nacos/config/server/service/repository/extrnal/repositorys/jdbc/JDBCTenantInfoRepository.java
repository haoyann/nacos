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

import com.alibaba.nacos.config.server.model.TenantInfo;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.TenantInfoRepository;
import com.alibaba.nacos.config.server.utils.LogUtil;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.util.Assert;

import java.util.Collections;
import java.util.List;

import static com.alibaba.nacos.config.server.service.repository.RowMapperManager.TENANT_INFO_ROW_MAPPER;

public class JDBCTenantInfoRepository extends BaseJDBCRepository implements TenantInfoRepository {
    
    private static final String SQL_TENANT_INFO_COUNT_BY_TENANT_ID = "select count(1) from tenant_info where tenant_id = ?";
    
    @Override
    public void insertTenantInfoAtomic(String kp, String tenantId, String tenantName, String tenantDesc,
            String createResoure, final long time) {
        try {
            jt.update(
                    "INSERT INTO tenant_info(kp,tenant_id,tenant_name,tenant_desc,create_source,gmt_create,gmt_modified) VALUES(?,?,?,?,?,?,?)",
                    kp, tenantId, tenantName, tenantDesc, createResoure, time, time);
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public void updateTenantNameAtomic(String kp, String tenantId, String tenantName, String tenantDesc) {
        try {
            jt.update(
                    "UPDATE tenant_info SET tenant_name = ?, tenant_desc = ?, gmt_modified= ? WHERE kp=? AND tenant_id=?",
                    tenantName, tenantDesc, System.currentTimeMillis(), kp, tenantId);
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public List<TenantInfo> findTenantByKp(String kp) {
        String sql = "SELECT tenant_id,tenant_name,tenant_desc FROM tenant_info WHERE kp=?";
        try {
            return this.jt.query(sql, new Object[] {kp}, TENANT_INFO_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        } catch (EmptyResultDataAccessException e) {
            return Collections.emptyList();
        } catch (Exception e) {
            LogUtil.FATAL_LOG.error("[db-other-error]" + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public TenantInfo findTenantByKp(String kp, String tenantId) {
        String sql = "SELECT tenant_id,tenant_name,tenant_desc FROM tenant_info WHERE kp=? AND tenant_id=?";
        try {
            return jt.queryForObject(sql, new Object[] {kp, tenantId}, TENANT_INFO_ROW_MAPPER);
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
    
    @Override
    public void removeTenantInfoAtomic(final String kp, final String tenantId) {
        try {
            jt.update("DELETE FROM tenant_info WHERE kp=? AND tenant_id=?", kp, tenantId);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public int tenantInfoCountByTenantId(String tenantId) {
        Assert.hasText(tenantId, "tenantId can not be null");
        Integer result = this.jt
                .queryForObject(SQL_TENANT_INFO_COUNT_BY_TENANT_ID, new String[] {tenantId}, Integer.class);
        if (result == null) {
            return 0;
        }
        return result.intValue();
    }
}
