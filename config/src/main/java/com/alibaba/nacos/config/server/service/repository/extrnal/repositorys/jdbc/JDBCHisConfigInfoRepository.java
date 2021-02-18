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

import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.model.ConfigHistoryInfo;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.service.repository.PaginationHelper;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.HisConfigInfoRepository;
import com.alibaba.nacos.config.server.utils.LogUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;

import java.sql.Timestamp;

import static com.alibaba.nacos.config.server.service.repository.RowMapperManager.HISTORY_DETAIL_ROW_MAPPER;
import static com.alibaba.nacos.config.server.service.repository.RowMapperManager.HISTORY_LIST_ROW_MAPPER;

public class JDBCHisConfigInfoRepository extends BaseJDBCRepository implements HisConfigInfoRepository {
    
    @Override
    public void insertConfigHistoryAtomic(long id, ConfigInfo configInfo, String srcIp, String srcUser, Timestamp time,
            String ops) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? StringUtils.EMPTY : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();
        final String md5Tmp = MD5Utils.md5Hex(configInfo.getContent(), Constants.ENCODE);
        try {
            jt.update(
                    "INSERT INTO his_config_info (id,data_id,group_id,tenant_id,app_name,content,md5,src_ip,src_user,gmt_modified,op_type) "
                            + "VALUES(?,?,?,?,?,?,?,?,?,?,?)", id, configInfo.getDataId(), configInfo.getGroup(),
                    tenantTmp, appNameTmp, configInfo.getContent(), md5Tmp, srcIp, srcUser, time, ops);
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public ConfigHistoryInfo detailConfigHistory(Long nid) {
        String sqlFetchRows = "SELECT nid,data_id,group_id,tenant_id,app_name,content,md5,src_user,src_ip,op_type,gmt_create,gmt_modified FROM his_config_info WHERE nid = ?";
        try {
            return jt.queryForObject(sqlFetchRows, new Object[] {nid}, HISTORY_DETAIL_ROW_MAPPER);
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[detail-config-history] error, nid:{}", new Object[] {nid}, e);
            throw e;
        }
    }
    
    @Override
    public ConfigHistoryInfo detailPreviousConfigHistory(Long id) {
        String sqlFetchRows = "SELECT nid,data_id,group_id,tenant_id,app_name,content,md5,src_user,src_ip,op_type,gmt_create,gmt_modified FROM his_config_info WHERE nid = (select max(nid) from his_config_info where id = ?) ";
        try {
            return jt.queryForObject(sqlFetchRows, new Object[] {id}, HISTORY_DETAIL_ROW_MAPPER);
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[detail-previous-config-history] error, id:{}", new Object[] {id}, e);
            throw e;
        }
    }
    
    @Override
    public void removeConfigHistory(Timestamp startTime, int limitSize) {
        String sql = "delete from his_config_info where gmt_modified < ? limit ?";
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        try {
            helper.updateLimit(sql, new Object[] {startTime, limitSize});
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public int findConfigHistoryCountByTime(Timestamp startTime) {
        String sql = "SELECT COUNT(*) FROM his_config_info WHERE gmt_modified < ?";
        Integer result = jt.queryForObject(sql, Integer.class, new Object[] {startTime});
        if (result == null) {
            throw new IllegalArgumentException("configInfoBetaCount error");
        }
        return result.intValue();
    }
    
    @Override
    public Page<ConfigHistoryInfo> findConfigHistory(String dataId, String group, String tenant, int pageNo,
            int pageSize) {
        PaginationHelper<ConfigHistoryInfo> helper = createPaginationHelper();
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        String sqlCountRows = "select count(*) from his_config_info where data_id = ? and group_id = ? and tenant_id = ?";
        String sqlFetchRows =
                "select nid,data_id,group_id,tenant_id,app_name,src_ip,src_user,op_type,gmt_create,gmt_modified from his_config_info "
                        + "where data_id = ? and group_id = ? and tenant_id = ? order by nid desc";
        
        Page<ConfigHistoryInfo> page = null;
        try {
            page = helper
                    .fetchPage(sqlCountRows, sqlFetchRows, new Object[] {dataId, group, tenantTmp}, pageNo, pageSize,
                            HISTORY_LIST_ROW_MAPPER);
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG
                    .error("[list-config-history] error, dataId:{}, group:{}", new Object[] {dataId, group}, e);
            throw e;
        }
        return page;
    }
    
}
