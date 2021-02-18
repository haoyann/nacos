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

package com.alibaba.nacos.config.server.service.repository.extrnal.repositorys;

import com.alibaba.nacos.config.server.model.ConfigAdvanceInfo;
import com.alibaba.nacos.config.server.model.ConfigAllInfo;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.model.ConfigInfoBase;
import com.alibaba.nacos.config.server.model.ConfigInfoWrapper;
import com.alibaba.nacos.config.server.model.Page;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

public interface ConfigInfoRepository {
    
    /**
     * Add configuration; database atomic operation, minimum sql action, no business encapsulation.
     *
     * @param configId          id
     * @param srcIp             ip
     * @param srcUser           user
     * @param configInfo        info
     * @param time              time
     * @param configAdvanceInfo advance info
     * @return execute sql result
     */
    long addConfigInfoAtomic(final long configId, final String srcIp, final String srcUser, final ConfigInfo configInfo,
            final Timestamp time, Map<String, Object> configAdvanceInfo);
    
    /**
     * Query configuration information; database atomic operation, minimum SQL action, no business encapsulation.
     *
     * @param dataId dataId
     * @param group  group
     * @param tenant tenant
     * @return config info
     */
    ConfigInfo findConfigInfo(final String dataId, final String group, final String tenant);
    
    
    /**
     * Update configuration; database atomic operation, minimum SQL action, no business encapsulation.
     *
     * @param configInfo        config info
     * @param srcIp             remote ip
     * @param srcUser           user
     * @param time              time
     * @param configAdvanceInfo advance info
     */
    void updateConfigInfoAtomic(final ConfigInfo configInfo, final String srcIp, final String srcUser,
            final Timestamp time, Map<String, Object> configAdvanceInfo);
    
    /**
     * update md5.
     *
     * @param dataId   data id
     * @param group    group
     * @param tenant   tenant
     * @param md5      md5
     * @param lastTime last modified time
     */
    void updateMd5(String dataId, String group, String tenant, String md5, Timestamp lastTime);
    
    /**
     * Remove configuration; database atomic operation, minimum SQL action, no business encapsulation.
     *
     * @param dataId  dataId
     * @param group   group
     * @param tenant  tenant
     * @param srcIp   ip
     * @param srcUser user
     */
    void removeConfigInfoAtomic(final String dataId, final String group, final String tenant, final String srcIp,
            final String srcUser);
    
    /**
     * find ConfigInfo by ids.
     *
     * @param ids id list
     * @return {@link com.alibaba.nacos.config.server.model.ConfigInfo} list
     * @author klw
     * @date 2019/7/5 16:37
     */
    List<ConfigInfo> findConfigInfosByIds(final String ids);
    
    /**
     * Remove configuration; database atomic operation, minimum SQL action, no business encapsulation.
     *
     * @param ids ids
     */
    void removeConfigInfoByIdsAtomic(final String ids);
    
    
    /**
     * Get the maxId.
     *
     * @return config max id
     */
    long findConfigMaxId();
    
    
    /**
     * Query common configuration information based on dataId and group.
     *
     * @param dataId  data id
     * @param group   group
     * @param tenant  tenant
     * @param appName app name
     * @return {@link ConfigInfo}
     */
    ConfigInfo findConfigInfoApp(final String dataId, final String group, final String tenant, final String appName);
    
    /**
     * Query configuration information based on dataId and group.
     *
     * @param dataId            data id
     * @param group             group
     * @param tenant            tenant
     * @param configAdvanceInfo advance info
     * @return {@link com.alibaba.nacos.config.server.Config}
     */
    ConfigInfo findConfigInfoAdvanceInfo(final String dataId, final String group, final String tenant,
            final Map<String, Object> configAdvanceInfo);
    
    /**
     * Query configuration information based on dataId and group.
     *
     * @param dataId data id
     * @param group  group
     * @return {@link ConfigInfoBase}
     */
    ConfigInfoBase findConfigInfoBase(final String dataId, final String group);
    
    /**
     * Query configuration information by primary key ID.
     *
     * @param id id
     * @return {@link ConfigInfo}
     */
    ConfigInfo findConfigInfo(long id);
    
    
    /**
     * Query configuration information based on dataId.
     *
     * @param pageNo   Page number (must be greater than 0)
     * @param pageSize Page size (must be greater than 0)
     * @param dataId   data id
     * @param tenant   tenant
     * @return {@link Page} with {@link ConfigInfo} generation
     */
    Page<ConfigInfo> findConfigInfoByDataId(final int pageNo, final int pageSize, final String dataId,
            final String tenant);
    
    /**
     * Query configuration information based on dataId.
     *
     * @param pageNo   Page number (must be greater than 0)
     * @param pageSize Page size (must be greater than 0)
     * @param dataId   data id
     * @param tenant   tenant
     * @param appName  app name
     * @return {@link Page} with {@link ConfigInfo} generation
     */
    Page<ConfigInfo> findConfigInfoByDataIdAndApp(final int pageNo, final int pageSize, final String dataId,
            final String tenant, final String appName);
    
    /**
     * find config info.
     *
     * @param pageNo            page number
     * @param pageSize          page size
     * @param dataId            data id
     * @param tenant            tenant
     * @param configAdvanceInfo advance info
     * @return {@link Page} with {@link ConfigInfo} generation
     */
    Page<ConfigInfo> findConfigInfoByDataIdAndAdvance(final int pageNo, final int pageSize, final String dataId,
            final String tenant, final Map<String, Object> configAdvanceInfo);
    
    /**
     * find config info.
     *
     * @param pageNo            page number
     * @param pageSize          page size
     * @param dataId            data id
     * @param group             group
     * @param tenant            tenant
     * @param configAdvanceInfo advance info
     * @return {@link Page} with {@link ConfigInfo} generation
     */
    Page<ConfigInfo> findConfigInfo4Page(final int pageNo, final int pageSize, final String dataId, final String group,
            final String tenant, final Map<String, Object> configAdvanceInfo);
    
    /**
     * Query configuration information based on dataId.
     *
     * @param pageNo   Page number (must be greater than 0)
     * @param pageSize Page size (must be greater than 0)
     * @param dataId   data id
     * @return {@link Page} with {@link ConfigInfoBase} generation
     */
    Page<ConfigInfoBase> findConfigInfoBaseByDataId(final int pageNo, final int pageSize, final String dataId);
    
    /**
     * Query configuration information based on group.
     *
     * @param pageNo   Page number (must be greater than 0)
     * @param pageSize Page size (must be greater than 0)
     * @param group    group
     * @param tenant   tenant
     * @return {@link Page} with {@link ConfigInfo} generation
     */
    Page<ConfigInfo> findConfigInfoByGroup(final int pageNo, final int pageSize, final String group,
            final String tenant);
    
    /**
     * Query configuration information based on group.
     *
     * @param pageNo   Page number (must be greater than 0)
     * @param pageSize Page size (must be greater than 0)
     * @param group    group
     * @param tenant   tenant
     * @param appName  app name
     * @return {@link Page} with {@link ConfigInfo} generation
     */
    Page<ConfigInfo> findConfigInfoByGroupAndApp(final int pageNo, final int pageSize, final String group,
            final String tenant, final String appName);
    
    /**
     * Query configuration information.
     *
     * @param pageNo            page number
     * @param pageSize          page size
     * @param group             group
     * @param tenant            tenant
     * @param configAdvanceInfo advance info
     * @return {@link Page} with {@link ConfigInfo} generation
     */
    Page<ConfigInfo> findConfigInfoByGroupAndAdvance(final int pageNo, final int pageSize, final String group,
            final String tenant, final Map<String, Object> configAdvanceInfo);
    
    /**
     * Query configuration information based on group.
     *
     * @param pageNo   Page number (must be greater than 0)
     * @param pageSize Page size (must be greater than 0)
     * @param tenant   tenant
     * @param appName  app name
     * @return {@link Page} with {@link ConfigInfo} generation
     */
    Page<ConfigInfo> findConfigInfoByApp(final int pageNo, final int pageSize, final String tenant,
            final String appName);
    
    /**
     * Query configuration information.
     *
     * @param pageNo            page number
     * @param pageSize          page size
     * @param tenant            tenant
     * @param configAdvanceInfo advance info
     * @return {@link Page} with {@link ConfigInfo} generation
     */
    Page<ConfigInfo> findConfigInfoByAdvance(final int pageNo, final int pageSize, final String tenant,
            final Map<String, Object> configAdvanceInfo);
    
    /**
     * Query configuration information based on group.
     *
     * @param pageNo   Page number (must be greater than 0)
     * @param pageSize Page size (must be greater than 0)
     * @param group    group
     * @return {@link Page} with {@link ConfigInfoBase} generation
     */
    Page<ConfigInfoBase> findConfigInfoBaseByGroup(final int pageNo, final int pageSize, final String group);
    
    /**
     * Returns the number of configuration items.
     *
     * @return number of configuration items.
     */
    int configInfoCount();
    
    /**
     * Returns the number of configuration items.
     *
     * @param tenant tenant
     * @return number of configuration items.
     */
    int configInfoCount(String tenant);
    
    /**
     * get tenant id list  by page.
     *
     * @param page     page number
     * @param pageSize page size
     * @return tenant id list
     */
    List<String> getTenantIdList(int page, int pageSize);
    
    /**
     * get group id list  by page.
     *
     * @param page     page number
     * @param pageSize page size
     * @return group id list
     */
    List<String> getGroupIdList(int page, int pageSize);
    
    /**
     * Query all configuration information by page.
     *
     * @param pageNo   Page number (starting at 1)
     * @param pageSize Page size (must be greater than 0)
     * @param tenant   tenant
     * @return {@link Page} with {@link ConfigInfo} generation
     */
    Page<ConfigInfo> findAllConfigInfo(final int pageNo, final int pageSize, final String tenant);
    
    /**
     * Query all configuration information by page for dump task.
     *
     * @param pageNo   page number
     * @param pageSize page size
     * @return {@link Page} with {@link ConfigInfoWrapper} generation
     */
    Page<ConfigInfoWrapper> findAllConfigInfoForDumpAll(final int pageNo, final int pageSize);
    
    /**
     * Query all config info.
     *
     * @param lastMaxId last max id
     * @param pageSize  page size
     * @return {@link Page} with {@link ConfigInfoWrapper} generation
     */
    Page<ConfigInfoWrapper> findAllConfigInfoFragment(final long lastMaxId, final int pageSize);
    
    
    /**
     * Query config info.
     *
     * @param pageNo            page number
     * @param pageSize          page size
     * @param dataId            data id
     * @param group             group
     * @param tenant            tenant
     * @param configAdvanceInfo advance info
     * @return {@link Page} with {@link ConfigInfo} generation
     */
    Page<ConfigInfo> findConfigInfoLike4Page(final int pageNo, final int pageSize, final String dataId,
            final String group, final String tenant, final Map<String, Object> configAdvanceInfo);
    
    /**
     * Query change config.
     *
     * @param startTime start time
     * @param endTime   end time
     * @return list
     */
    List<Map<String, Object>> findChangeConfig(final Timestamp startTime, final Timestamp endTime);
    
    
    /**
     * Query deleted config.
     *
     * @param startTime start time
     * @param endTime   end time
     * @return list
     */
    List<Map<String, Object>> findDeletedConfig(final Timestamp startTime, final Timestamp endTime);
    
    
    /**
     * Query configuration information; database atomic operation, minimum SQL action, no business encapsulation.
     *
     * @param dataId dataId
     * @param group  group
     * @param tenant tenant
     * @return advance info
     */
    ConfigAdvanceInfo findConfigAdvanceInfo(final String dataId, final String group, final String tenant);
    
    /**
     * Query configuration information; database atomic operation, minimum SQL action, no business encapsulation.
     *
     * @param dataId dataId
     * @param group  group
     * @param tenant tenant
     * @return advance info
     */
    ConfigAllInfo findConfigAllInfo(final String dataId, final String group, final String tenant);
    
    /**
     * list group key md5 by page.
     *
     * @param pageNo   page no
     * @param pageSize page size
     * @return {@link ConfigInfoWrapper} list
     */
    List<ConfigInfoWrapper> listGroupKeyMd5ByPage(int pageNo, int pageSize);
    
    /**
     * Query config info.
     *
     * @param dataId data id
     * @param group  group
     * @param tenant tenant
     * @return {@link ConfigInfoWrapper}
     */
    ConfigInfoWrapper queryConfigInfo(final String dataId, final String group, final String tenant);
    
    /**
     * query all configuration information according to group, appName, tenant (for export).
     *
     * @param dataId  data id
     * @param group   group
     * @param tenant  tenant
     * @param appName appName
     * @param ids     ids
     * @return Collection of ConfigInfo objects
     */
    List<ConfigAllInfo> findAllConfigInfo4Export(final String dataId, final String group, final String tenant,
            final String appName, final List<Long> ids);
}
