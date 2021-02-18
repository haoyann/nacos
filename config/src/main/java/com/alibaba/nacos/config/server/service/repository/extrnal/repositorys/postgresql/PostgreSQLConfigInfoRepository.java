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

package com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.postgresql;

import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.jdbc.JDBCConfigInfoRepository;
import com.alibaba.nacos.config.server.utils.LogUtil;
import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import javax.annotation.PostConstruct;
import java.sql.Timestamp;
import java.util.Map;

public class PostgreSQLConfigInfoRepository extends JDBCConfigInfoRepository {
    
    protected NamedParameterJdbcTemplate namedParameterJdbcTemplate;
    
    @PostConstruct
    public void createNamedJdbcTemplate() {
        namedParameterJdbcTemplate = new NamedParameterJdbcTemplate(jt);
    }
    
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
                        + "gmt_modified,c_desc,c_use,effect,type,c_schema) VALUES(:data_id,:group_id,:tenant_id,:app_name,"
                        + ":content,:md5,:src_ip,:src_user,:gmt_create,:gmt_modified,:c_desc,:c_use,:effect,:type,:c_schema)";
        try {
            MapSqlParameterSource data = new MapSqlParameterSource();
            data.addValue("data_id", configInfo.getDataId());
            data.addValue("group_id", configInfo.getGroup());
            data.addValue("tenant_id", tenantTmp);
            data.addValue("app_name", appNameTmp);
            data.addValue("content", configInfo.getContent());
            data.addValue("md5", md5Tmp);
            data.addValue("src_ip", srcIp);
            data.addValue("src_user", srcUser);
            data.addValue("gmt_create", time);
            data.addValue("gmt_modified", time);
            data.addValue("c_desc", desc);
            data.addValue("c_use", use);
            data.addValue("effect", effect);
            data.addValue("type", type);
            data.addValue("c_schema", schema);
            namedParameterJdbcTemplate.update(sql, data, keyHolder, new String[] {"id"});
            Number nu = keyHolder.getKey();
            if (nu == null) {
                throw new IllegalArgumentException("insert config_info fail");
            }
            return nu.longValue();
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            e.printStackTrace();
            throw e;
        }
    }
    
}
