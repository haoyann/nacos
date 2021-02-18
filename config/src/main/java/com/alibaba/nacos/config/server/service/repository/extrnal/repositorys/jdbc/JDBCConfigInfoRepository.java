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
import com.alibaba.nacos.config.server.model.ConfigAdvanceInfo;
import com.alibaba.nacos.config.server.model.ConfigAllInfo;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.model.ConfigInfoBase;
import com.alibaba.nacos.config.server.model.ConfigInfoWrapper;
import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.service.repository.PaginationHelper;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.ConfigInfoRepository;
import com.alibaba.nacos.config.server.utils.LogUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.nacos.config.server.service.repository.RowMapperManager.CONFIG_ADVANCE_INFO_ROW_MAPPER;
import static com.alibaba.nacos.config.server.service.repository.RowMapperManager.CONFIG_ALL_INFO_ROW_MAPPER;
import static com.alibaba.nacos.config.server.service.repository.RowMapperManager.CONFIG_INFO_BASE_ROW_MAPPER;
import static com.alibaba.nacos.config.server.service.repository.RowMapperManager.CONFIG_INFO_ROW_MAPPER;
import static com.alibaba.nacos.config.server.service.repository.RowMapperManager.CONFIG_INFO_WRAPPER_ROW_MAPPER;

public class JDBCConfigInfoRepository extends BaseJDBCRepository implements ConfigInfoRepository {
    
    
    protected static final String SQL_DELETE_CONFIG_INFO_BY_IDS = "DELETE FROM config_info WHERE ";
    
    protected static final String SQL_FIND_CONFIG_INFO_BY_IDS = "SELECT ID,data_id,group_id,tenant_id,app_name,content,md5 FROM config_info WHERE ";
    
    protected static final String SQL_FIND_ALL_CONFIG_INFO = "select id,data_id,group_id,tenant_id,app_name,content,type,md5,gmt_create,gmt_modified,src_user,src_ip,c_desc,c_use,effect,c_schema from config_info";
    
    
    @Override
    public long addConfigInfoAtomic(long configId, String srcIp, String srcUser, ConfigInfo configInfo, Timestamp time,
            Map<String, Object> configAdvanceInfo) {
        final String appNameTmp =
                StringUtils.isBlank(configInfo.getAppName()) ? StringUtils.EMPTY : configInfo.getAppName();
        final String tenantTmp =
                StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();
        
        final String desc = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("desc");
        final String use = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("use");
        final String effect = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("effect");
        final String type = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("type");
        final String schema = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("schema");
        
        final String md5Tmp = MD5Utils.md5Hex(configInfo.getContent(), Constants.ENCODE);
        
        KeyHolder keyHolder = new GeneratedKeyHolder();
        
        final String sql =
                "INSERT INTO config_info(data_id,group_id,tenant_id,app_name,content,md5,src_ip,src_user,gmt_create,"
                        + "gmt_modified,c_desc,c_use,effect,type,c_schema) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        
        try {
            jt.update(new PreparedStatementCreator() {
                @Override
                public PreparedStatement createPreparedStatement(Connection connection) throws SQLException {
                    PreparedStatement ps = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                    ps.setString(1, configInfo.getDataId());
                    ps.setString(2, configInfo.getGroup());
                    ps.setString(3, tenantTmp);
                    ps.setString(4, appNameTmp);
                    ps.setString(5, configInfo.getContent());
                    ps.setString(6, md5Tmp);
                    ps.setString(7, srcIp);
                    ps.setString(8, srcUser);
                    ps.setTimestamp(9, time);
                    ps.setTimestamp(10, time);
                    ps.setString(11, desc);
                    ps.setString(12, use);
                    ps.setString(13, effect);
                    ps.setString(14, type);
                    ps.setString(15, schema);
                    return ps;
                }
            }, keyHolder);
            Number nu = keyHolder.getKey();
            if (nu == null) {
                throw new IllegalArgumentException("insert config_info fail");
            }
            return nu.longValue();
        } catch (
        
                CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public ConfigInfo findConfigInfo(String dataId, String group, String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        try {
            return this.jt.queryForObject(
                    "SELECT ID,data_id,group_id,tenant_id,app_name,content,md5,type FROM config_info WHERE data_id=? AND group_id=? AND tenant_id=?",
                    new Object[] {dataId, group, tenantTmp}, CONFIG_INFO_ROW_MAPPER);
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null.
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public void updateConfigInfoAtomic(ConfigInfo configInfo, String srcIp, String srcUser, Timestamp time,
            Map<String, Object> configAdvanceInfo) {
        String appNameTmp = StringUtils.isBlank(configInfo.getAppName()) ? StringUtils.EMPTY : configInfo.getAppName();
        String tenantTmp = StringUtils.isBlank(configInfo.getTenant()) ? StringUtils.EMPTY : configInfo.getTenant();
        final String md5Tmp = MD5Utils.md5Hex(configInfo.getContent(), Constants.ENCODE);
        String desc = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("desc");
        String use = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("use");
        String effect = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("effect");
        String type = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("type");
        String schema = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("schema");
        
        try {
            jt.update("UPDATE config_info SET content=?, md5 = ?, src_ip=?,src_user=?,gmt_modified=?,"
                            + "app_name=?,c_desc=?,c_use=?,effect=?,type=?,c_schema=? "
                            + "WHERE data_id=? AND group_id=? AND tenant_id=?", configInfo.getContent(), md5Tmp, srcIp, srcUser,
                    time, appNameTmp, desc, use, effect, type, schema, configInfo.getDataId(), configInfo.getGroup(),
                    tenantTmp);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public void updateMd5(String dataId, String group, String tenant, String md5, Timestamp lastTime) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        try {
            jt.update(
                    "UPDATE config_info SET md5 = ? WHERE data_id=? AND group_id=? AND tenant_id=? AND gmt_modified=?",
                    md5, dataId, group, tenantTmp, lastTime);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public void removeConfigInfoAtomic(String dataId, String group, String tenant, String srcIp, String srcUser) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        try {
            jt.update("DELETE FROM config_info WHERE data_id=? AND group_id=? AND tenant_id=?", dataId, group,
                    tenantTmp);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public List<ConfigInfo> findConfigInfosByIds(String ids) {
        if (StringUtils.isBlank(ids)) {
            return null;
        }
        StringBuilder sql = new StringBuilder(SQL_FIND_CONFIG_INFO_BY_IDS);
        sql.append("id in (");
        List<Long> paramList = new ArrayList<>();
        String[] tagArr = ids.split(",");
        for (int i = 0; i < tagArr.length; i++) {
            if (i != 0) {
                sql.append(", ");
            }
            sql.append("?");
            paramList.add(Long.parseLong(tagArr[i]));
        }
        sql.append(") ");
        try {
            return this.jt.query(sql.toString(), paramList.toArray(), CONFIG_INFO_ROW_MAPPER);
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public void removeConfigInfoByIdsAtomic(String ids) {
        if (StringUtils.isBlank(ids)) {
            return;
        }
        StringBuilder sql = new StringBuilder(SQL_DELETE_CONFIG_INFO_BY_IDS);
        sql.append("id in (");
        List<Long> paramList = new ArrayList<>();
        String[] tagArr = ids.split(",");
        for (int i = 0; i < tagArr.length; i++) {
            if (i != 0) {
                sql.append(", ");
            }
            sql.append("?");
            paramList.add(Long.parseLong(tagArr[i]));
        }
        sql.append(") ");
        try {
            jt.update(sql.toString(), paramList.toArray());
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public long findConfigMaxId() {
        String sql = "SELECT max(id) FROM config_info";
        try {
            return jt.queryForObject(sql, Long.class);
        } catch (NullPointerException e) {
            return 0;
        }
        
    }
    
    @Override
    public ConfigInfo findConfigInfoApp(final String dataId, final String group, final String tenant,
            final String appName) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        try {
            return this.jt.queryForObject(
                    "SELECT ID,data_id,group_id,tenant_id,app_name,content FROM config_info WHERE data_id=? AND group_id=? AND tenant_id=? AND app_name=?",
                    new Object[] {dataId, group, tenantTmp, appName}, CONFIG_INFO_ROW_MAPPER);
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null.
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public ConfigInfo findConfigInfoAdvanceInfo(final String dataId, final String group, final String tenant,
            final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        final String appName = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("appName");
        final String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
        List<String> paramList = new ArrayList<String>();
        paramList.add(dataId);
        paramList.add(group);
        paramList.add(tenantTmp);
        
        StringBuilder sql = new StringBuilder(
                "select ID,data_id,group_id,tenant_id,app_name,content from config_info where data_id=? and group_id=? and tenant_id=? ");
        if (StringUtils.isNotBlank(configTags)) {
            sql = new StringBuilder(
                    "select a.ID,a.data_id,a.group_id,a.tenant_id,a.app_name,a.content from config_info  a left join "
                            + "config_tags_relation b on a.id=b.id where a.data_id=? and a.group_id=? and a.tenant_id=? ");
            sql.append(" and b.tag_name in (");
            String[] tagArr = configTags.split(",");
            for (int i = 0; i < tagArr.length; i++) {
                if (i != 0) {
                    sql.append(", ");
                }
                sql.append("?");
                paramList.add(tagArr[i]);
            }
            sql.append(") ");
            
            if (StringUtils.isNotBlank(appName)) {
                sql.append(" and a.app_name=? ");
                paramList.add(appName);
            }
        } else {
            if (StringUtils.isNotBlank(appName)) {
                sql.append(" and app_name=? ");
                paramList.add(appName);
            }
        }
        
        try {
            return this.jt.queryForObject(sql.toString(), paramList.toArray(), CONFIG_INFO_ROW_MAPPER);
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null.
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
        
    }
    
    @Override
    public ConfigInfoBase findConfigInfoBase(final String dataId, final String group) {
        try {
            return this.jt.queryForObject(
                    "SELECT ID,data_id,group_id,content FROM config_info WHERE data_id=? AND group_id=? AND tenant_id=?",
                    new Object[] {dataId, group, StringUtils.EMPTY}, CONFIG_INFO_BASE_ROW_MAPPER);
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null.
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public ConfigInfo findConfigInfo(long id) {
        try {
            return this.jt
                    .queryForObject("SELECT ID,data_id,group_id,tenant_id,app_name,content FROM config_info WHERE ID=?",
                            new Object[] {id}, CONFIG_INFO_ROW_MAPPER);
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null.
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    
    @Override
    public Page<ConfigInfo> findConfigInfoByDataId(final int pageNo, final int pageSize, final String dataId,
            final String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        try {
            return helper.fetchPage("select count(*) from config_info where data_id=? and tenant_id=?",
                    "select ID,data_id,group_id,tenant_id,app_name,content from config_info where data_id=? and tenant_id=?",
                    new Object[] {dataId, tenantTmp}, pageNo, pageSize, CONFIG_INFO_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoByDataIdAndApp(final int pageNo, final int pageSize, final String dataId,
            final String tenant, final String appName) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        try {
            return helper.fetchPage("select count(*) from config_info where data_id=? and tenant_id=? and app_name=?",
                    "select ID,data_id,group_id,tenant_id,app_name,content from config_info where data_id=? and tenant_id=? and app_name=?",
                    new Object[] {dataId, tenantTmp, appName}, pageNo, pageSize, CONFIG_INFO_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoByDataIdAndAdvance(final int pageNo, final int pageSize, final String dataId,
            final String tenant, final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        final String appName = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("appName");
        final String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
        StringBuilder sqlCount = new StringBuilder("select count(*) from config_info where data_id=? and tenant_id=? ");
        StringBuilder sql = new StringBuilder(
                "select ID,data_id,group_id,tenant_id,app_name,content from config_info where data_id=? and tenant_id=? ");
        List<String> paramList = new ArrayList<String>();
        paramList.add(dataId);
        paramList.add(tenantTmp);
        if (StringUtils.isNotBlank(configTags)) {
            sqlCount = new StringBuilder(
                    "select count(*) from config_info  a left join config_tags_relation b on a.id=b.id where a.data_id=? and a.tenant_id=? ");
            
            sql = new StringBuilder(
                    "select a.ID,a.data_id,a.group_id,a.tenant_id,a.app_name,a.content from config_info  a left join "
                            + "config_tags_relation b on a.id=b.id where a.data_id=? and a.tenant_id=? ");
            
            sqlCount.append(" and b.tag_name in (");
            sql.append(" and b.tag_name in (");
            String[] tagArr = configTags.split(",");
            for (int i = 0; i < tagArr.length; i++) {
                if (i != 0) {
                    sqlCount.append(", ");
                    sql.append(", ");
                }
                sqlCount.append("?");
                sql.append("?");
                paramList.add(tagArr[i]);
            }
            sqlCount.append(") ");
            sql.append(") ");
            
            if (StringUtils.isNotBlank(appName)) {
                sqlCount.append(" and a.app_name=? ");
                sql.append(" and a.app_name=? ");
                paramList.add(appName);
            }
        } else {
            if (StringUtils.isNotBlank(appName)) {
                sqlCount.append(" and app_name=? ");
                sql.append(" and app_name=? ");
                paramList.add(appName);
            }
        }
        try {
            return helper.fetchPage(sqlCount.toString(), sql.toString(), paramList.toArray(), pageNo, pageSize,
                    CONFIG_INFO_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfo4Page(final int pageNo, final int pageSize, final String dataId,
            final String group, final String tenant, final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        final String appName = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("appName");
        final String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
        String sqlCount = "select count(*) from config_info";
        String sql = "select ID,data_id,group_id,tenant_id,app_name,content,type from config_info";
        StringBuilder where = new StringBuilder(" where ");
        List<String> paramList = new ArrayList<String>();
        paramList.add(tenantTmp);
        if (StringUtils.isNotBlank(configTags)) {
            sqlCount = "select count(*) from config_info  a left join config_tags_relation b on a.id=b.id";
            sql = "select a.ID,a.data_id,a.group_id,a.tenant_id,a.app_name,a.content from config_info  a left join "
                    + "config_tags_relation b on a.id=b.id";
            
            where.append(" a.tenant_id=? ");
            
            if (StringUtils.isNotBlank(dataId)) {
                where.append(" and a.data_id=? ");
                paramList.add(dataId);
            }
            if (StringUtils.isNotBlank(group)) {
                where.append(" and a.group_id=? ");
                paramList.add(group);
            }
            if (StringUtils.isNotBlank(appName)) {
                where.append(" and a.app_name=? ");
                paramList.add(appName);
            }
            
            where.append(" and b.tag_name in (");
            String[] tagArr = configTags.split(",");
            for (int i = 0; i < tagArr.length; i++) {
                if (i != 0) {
                    where.append(", ");
                }
                where.append("?");
                paramList.add(tagArr[i]);
            }
            where.append(") ");
        } else {
            where.append(" tenant_id=? ");
            if (StringUtils.isNotBlank(dataId)) {
                where.append(" and data_id=? ");
                paramList.add(dataId);
            }
            if (StringUtils.isNotBlank(group)) {
                where.append(" and group_id=? ");
                paramList.add(group);
            }
            if (StringUtils.isNotBlank(appName)) {
                where.append(" and app_name=? ");
                paramList.add(appName);
            }
        }
        try {
            return helper.fetchPage(sqlCount + where, sql + where, paramList.toArray(), pageNo, pageSize,
                    CONFIG_INFO_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public Page<ConfigInfoBase> findConfigInfoBaseByDataId(final int pageNo, final int pageSize, final String dataId) {
        PaginationHelper<ConfigInfoBase> helper = createPaginationHelper();
        try {
            return helper.fetchPage("select count(*) from config_info where data_id=? and tenant_id=?",
                    "select ID,data_id,group_id,content from config_info where data_id=? and tenant_id=?",
                    new Object[] {dataId, StringUtils.EMPTY}, pageNo, pageSize, CONFIG_INFO_BASE_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoByGroup(final int pageNo, final int pageSize, final String group,
            final String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        try {
            return helper.fetchPage("select count(*) from config_info where group_id=? and tenant_id=?",
                    "select ID,data_id,group_id,tenant_id,app_name,content from config_info where group_id=? and tenant_id=?",
                    new Object[] {group, tenantTmp}, pageNo, pageSize, CONFIG_INFO_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoByGroupAndApp(final int pageNo, final int pageSize, final String group,
            final String tenant, final String appName) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        try {
            return helper.fetchPage("select count(*) from config_info where group_id=? and tenant_id=? and app_name =?",
                    "select ID,data_id,group_id,tenant_id,app_name,content from config_info where group_id=? and tenant_id=? and app_name =?",
                    new Object[] {group, tenantTmp, appName}, pageNo, pageSize, CONFIG_INFO_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoByGroupAndAdvance(final int pageNo, final int pageSize, final String group,
            final String tenant, final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        
        final String appName = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("appName");
        final String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
        StringBuilder sqlCount = new StringBuilder(
                "select count(*) from config_info where group_id=? and tenant_id=? ");
        StringBuilder sql = new StringBuilder(
                "select ID,data_id,group_id,tenant_id,app_name,content from config_info where group_id=? and tenant_id=? ");
        List<String> paramList = new ArrayList<String>();
        paramList.add(group);
        paramList.add(tenantTmp);
        if (StringUtils.isNotBlank(configTags)) {
            sqlCount = new StringBuilder(
                    "select count(*) from config_info  a left join config_tags_relation b on a.id=b.id where a.group_id=? and a.tenant_id=? ");
            sql = new StringBuilder(
                    "select a.ID,a.data_id,a.group_id,a.tenant_id,a.app_name,a.content from config_info  a left join "
                            + "config_tags_relation b on a.id=b.id where a.group_id=? and a.tenant_id=? ");
            
            sqlCount.append(" and b.tag_name in (");
            sql.append(" and b.tag_name in (");
            String[] tagArr = configTags.split(",");
            for (int i = 0; i < tagArr.length; i++) {
                if (i != 0) {
                    sqlCount.append(", ");
                    sql.append(", ");
                }
                sqlCount.append("?");
                sql.append("?");
                paramList.add(tagArr[i]);
            }
            sqlCount.append(") ");
            sql.append(") ");
            
            if (StringUtils.isNotBlank(appName)) {
                sqlCount.append(" and a.app_name=? ");
                sql.append(" and a.app_name=? ");
                paramList.add(appName);
            }
        } else {
            if (StringUtils.isNotBlank(appName)) {
                sqlCount.append(" and app_name=? ");
                sql.append(" and app_name=? ");
                paramList.add(appName);
            }
        }
        
        try {
            return helper.fetchPage(sqlCount.toString(), sql.toString(), paramList.toArray(), pageNo, pageSize,
                    CONFIG_INFO_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoByApp(final int pageNo, final int pageSize, final String tenant,
            final String appName) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        try {
            return helper.fetchPage("select count(*) from config_info where tenant_id like ? and app_name=?",
                    "select ID,data_id,group_id,tenant_id,app_name,content from config_info where tenant_id like ? and app_name=?",
                    new Object[] {generateLikeArgument(tenantTmp), appName}, pageNo, pageSize, CONFIG_INFO_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoByAdvance(final int pageNo, final int pageSize, final String tenant,
            final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        final String appName = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("appName");
        final String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
        StringBuilder sqlCount = new StringBuilder("select count(*) from config_info where tenant_id like ? ");
        StringBuilder sql = new StringBuilder(
                "select ID,data_id,group_id,tenant_id,app_name,content from config_info where tenant_id like ? ");
        List<String> paramList = new ArrayList<String>();
        paramList.add(tenantTmp);
        if (StringUtils.isNotBlank(configTags)) {
            sqlCount = new StringBuilder(
                    "select count(*) from config_info a left join config_tags_relation b on a.id=b.id where a.tenant_id=? ");
            
            sql = new StringBuilder(
                    "select a.ID,a.data_id,a.group_id,a.tenant_id,a.app_name,a.content from config_info  a left join "
                            + "config_tags_relation b on a.id=b.id where a.tenant_id=? ");
            
            sqlCount.append(" and b.tag_name in (");
            sql.append(" and b.tag_name in (");
            String[] tagArr = configTags.split(",");
            for (int i = 0; i < tagArr.length; i++) {
                if (i != 0) {
                    sqlCount.append(", ");
                    sql.append(", ");
                }
                sqlCount.append("?");
                sql.append("?");
                paramList.add(tagArr[i]);
            }
            sqlCount.append(") ");
            sql.append(") ");
            
            if (StringUtils.isNotBlank(appName)) {
                sqlCount.append(" and a.app_name=? ");
                sql.append(" and a.app_name=? ");
                paramList.add(appName);
            }
        } else {
            if (StringUtils.isNotBlank(appName)) {
                sqlCount.append(" and app_name=? ");
                sql.append(" and app_name=? ");
                paramList.add(appName);
            }
        }
        
        try {
            return helper.fetchPage(sqlCount.toString(), sql.toString(), paramList.toArray(), pageNo, pageSize,
                    CONFIG_INFO_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public Page<ConfigInfoBase> findConfigInfoBaseByGroup(final int pageNo, final int pageSize, final String group) {
        PaginationHelper<ConfigInfoBase> helper = createPaginationHelper();
        try {
            return helper.fetchPage("select count(*) from config_info where group_id=? and tenant_id=?",
                    "select ID,data_id,group_id,content from config_info where group_id=? and tenant_id=?",
                    new Object[] {group, StringUtils.EMPTY}, pageNo, pageSize, CONFIG_INFO_BASE_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public int configInfoCount() {
        String sql = " SELECT COUNT(ID) FROM config_info ";
        Integer result = jt.queryForObject(sql, Integer.class);
        if (result == null) {
            throw new IllegalArgumentException("configInfoCount error");
        }
        return result.intValue();
    }
    
    @Override
    public int configInfoCount(String tenant) {
        String sql = " SELECT COUNT(ID) FROM config_info where tenant_id like ?";
        Integer result = jt.queryForObject(sql, new Object[] {tenant}, Integer.class);
        if (result == null) {
            throw new IllegalArgumentException("configInfoCount error");
        }
        return result.intValue();
    }
    
    @Override
    public List<String> getTenantIdList(int page, int pageSize) {
        String sql = "SELECT tenant_id FROM config_info WHERE tenant_id != '' GROUP BY tenant_id LIMIT ?, ?";
        int from = (page - 1) * pageSize;
        return jt.queryForList(sql, String.class, from, pageSize);
    }
    
    @Override
    public List<String> getGroupIdList(int page, int pageSize) {
        String sql = "SELECT group_id FROM config_info WHERE tenant_id ='' GROUP BY group_id LIMIT ?, ?";
        int from = (page - 1) * pageSize;
        return jt.queryForList(sql, String.class, from, pageSize);
    }
    
    @Override
    public Page<ConfigInfo> findAllConfigInfo(int pageNo, int pageSize, String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        String sqlCountRows = "SELECT COUNT(*) FROM config_info";
        String sqlFetchRows = " SELECT t.id,data_id,group_id,tenant_id,app_name,content,md5 "
                + " FROM (  SELECT id FROM config_info WHERE tenant_id like ? ORDER BY id LIMIT ?,? )"
                + " g, config_info t  WHERE g.id = t.id ";
        
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        try {
            return helper.fetchPageLimit(sqlCountRows, sqlFetchRows,
                    new Object[] {generateLikeArgument(tenantTmp), (pageNo - 1) * pageSize, pageSize}, pageNo, pageSize,
                    CONFIG_INFO_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public Page<ConfigInfoWrapper> findAllConfigInfoForDumpAll(final int pageNo, final int pageSize) {
        String sqlCountRows = "select count(*) from config_info";
        String sqlFetchRows = " SELECT t.id,type,data_id,group_id,tenant_id,app_name,content,md5,gmt_modified "
                + " FROM ( SELECT id FROM config_info   ORDER BY id LIMIT ?,?  )"
                + " g, config_info t WHERE g.id = t.id ";
        PaginationHelper<ConfigInfoWrapper> helper = createPaginationHelper();
        
        List<String> params = new ArrayList<String>();
        
        try {
            return helper.fetchPageLimit(sqlCountRows, sqlFetchRows, params.toArray(), pageNo, pageSize,
                    CONFIG_INFO_WRAPPER_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public Page<ConfigInfoWrapper> findAllConfigInfoFragment(final long lastMaxId, final int pageSize) {
        String select = "SELECT id,data_id,group_id,tenant_id,app_name,content,md5,gmt_modified,type from config_info where id > ? order by id asc limit ?,?";
        PaginationHelper<ConfigInfoWrapper> helper = createPaginationHelper();
        try {
            return helper.fetchPageLimit(select, new Object[] {lastMaxId, 0, pageSize}, 1, pageSize,
                    CONFIG_INFO_WRAPPER_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    
    @Override
    public Page<ConfigInfo> findConfigInfoLike4Page(final int pageNo, final int pageSize, final String dataId,
            final String group, final String tenant, final Map<String, Object> configAdvanceInfo) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        final String appName = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("appName");
        final String content = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("content");
        final String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        String sqlCountRows = "select count(*) from config_info";
        String sqlFetchRows = "select ID,data_id,group_id,tenant_id,app_name,content from config_info";
        StringBuilder where = new StringBuilder(" where ");
        List<String> params = new ArrayList<String>();
        params.add(generateLikeArgument(tenantTmp));
        if (StringUtils.isNotBlank(configTags)) {
            sqlCountRows = "select count(*) from config_info  a left join config_tags_relation b on a.id=b.id ";
            sqlFetchRows = "select a.ID,a.data_id,a.group_id,a.tenant_id,a.app_name,a.content from config_info a left join config_tags_relation b on a.id=b.id ";
            
            where.append(" a.tenant_id like ? ");
            if (!StringUtils.isBlank(dataId)) {
                where.append(" and a.data_id like ? ");
                params.add(generateLikeArgument(dataId));
            }
            if (!StringUtils.isBlank(group)) {
                where.append(" and a.group_id like ? ");
                params.add(generateLikeArgument(group));
            }
            if (!StringUtils.isBlank(appName)) {
                where.append(" and a.app_name = ? ");
                params.add(appName);
            }
            if (!StringUtils.isBlank(content)) {
                where.append(" and a.content like ? ");
                params.add(generateLikeArgument(content));
            }
            
            where.append(" and b.tag_name in (");
            String[] tagArr = configTags.split(",");
            for (int i = 0; i < tagArr.length; i++) {
                if (i != 0) {
                    where.append(", ");
                }
                where.append("?");
                params.add(tagArr[i]);
            }
            where.append(") ");
        } else {
            where.append(" tenant_id like ? ");
            if (!StringUtils.isBlank(dataId)) {
                where.append(" and data_id like ? ");
                params.add(generateLikeArgument(dataId));
            }
            if (!StringUtils.isBlank(group)) {
                where.append(" and group_id like ? ");
                params.add(generateLikeArgument(group));
            }
            if (!StringUtils.isBlank(appName)) {
                where.append(" and app_name = ? ");
                params.add(appName);
            }
            if (!StringUtils.isBlank(content)) {
                where.append(" and content like ? ");
                params.add(generateLikeArgument(content));
            }
        }
        
        try {
            return helper.fetchPage(sqlCountRows + where, sqlFetchRows + where, params.toArray(), pageNo, pageSize,
                    CONFIG_INFO_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public List<Map<String, Object>> findChangeConfig(final Timestamp startTime, final Timestamp endTime) {
        try {
            return jt.queryForList(
                    "SELECT data_id, group_id, tenant_id, app_name, content, gmt_modified FROM config_info WHERE gmt_modified >=? AND gmt_modified <= ?",
                    new Object[] {startTime, endTime});
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public List<Map<String, Object>> findDeletedConfig(Timestamp startTime, Timestamp endTime) {
        try {
            return jt.queryForList(
                    "SELECT DISTINCT data_id, group_id, tenant_id FROM his_config_info WHERE op_type = 'D' AND gmt_modified >=? AND gmt_modified <= ?",
                    new Object[] {startTime, endTime});
        } catch (DataAccessException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public ConfigAdvanceInfo findConfigAdvanceInfo(final String dataId, final String group, final String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        return this.jt.queryForObject(
                "SELECT gmt_create,gmt_modified,src_user,src_ip,c_desc,c_use,effect,type,c_schema FROM config_info WHERE data_id=? AND group_id=? AND tenant_id=?",
                new Object[] {dataId, group, tenantTmp}, CONFIG_ADVANCE_INFO_ROW_MAPPER);
        
    }
    
    @Override
    public ConfigAllInfo findConfigAllInfo(final String dataId, final String group, final String tenant) {
        final String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        return this.jt.queryForObject("SELECT ID,data_id,group_id,tenant_id,app_name,content,md5,"
                        + "gmt_create,gmt_modified,src_user,src_ip,c_desc,c_use,effect,type,c_schema FROM config_info "
                        + "WHERE data_id=? AND group_id=? AND tenant_id=?", new Object[] {dataId, group, tenantTmp},
                CONFIG_ALL_INFO_ROW_MAPPER);
        
    }
    
    @Override
    public List<ConfigInfoWrapper> listGroupKeyMd5ByPage(int pageNo, int pageSize) {
        String sqlCountRows = " SELECT COUNT(*) FROM config_info ";
        String sqlFetchRows = " SELECT t.id,data_id,group_id,tenant_id,app_name,md5,type,gmt_modified FROM "
                + "( SELECT id FROM config_info ORDER BY id LIMIT ?,?  ) g, config_info t WHERE g.id = t.id";
        PaginationHelper<ConfigInfoWrapper> helper = createPaginationHelper();
        try {
            Page<ConfigInfoWrapper> page = helper
                    .fetchPageLimit(sqlCountRows, sqlFetchRows, new Object[] {(pageNo - 1) * pageSize, pageSize},
                            pageNo, pageSize, CONFIG_INFO_WRAPPER_ROW_MAPPER);
            
            return page.getPageItems();
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public ConfigInfoWrapper queryConfigInfo(final String dataId, final String group, final String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        try {
            return this.jt.queryForObject(
                    "SELECT ID,data_id,group_id,tenant_id,app_name,content,type,gmt_modified,md5 FROM config_info "
                            + "WHERE data_id=? AND group_id=? AND tenant_id=?", new Object[] {dataId, group, tenantTmp},
                    CONFIG_INFO_WRAPPER_ROW_MAPPER);
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public List<ConfigAllInfo> findAllConfigInfo4Export(final String dataId, final String group, final String tenant,
            final String appName, final List<Long> ids) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        StringBuilder where = new StringBuilder(" where ");
        List<Object> paramList = new ArrayList<>();
        if (!CollectionUtils.isEmpty(ids)) {
            where.append(" id in (");
            for (int i = 0; i < ids.size(); i++) {
                if (i != 0) {
                    where.append(", ");
                }
                where.append("?");
                paramList.add(ids.get(i));
            }
            where.append(") ");
        } else {
            where.append(" tenant_id=? ");
            paramList.add(tenantTmp);
            if (!StringUtils.isBlank(dataId)) {
                where.append(" and data_id like ? ");
                paramList.add(generateLikeArgument(dataId));
            }
            if (StringUtils.isNotBlank(group)) {
                where.append(" and group_id=? ");
                paramList.add(group);
            }
            if (StringUtils.isNotBlank(appName)) {
                where.append(" and app_name=? ");
                paramList.add(appName);
            }
        }
        try {
            return this.jt.query(SQL_FIND_ALL_CONFIG_INFO + where, paramList.toArray(), CONFIG_ALL_INFO_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
}