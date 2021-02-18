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

package com.alibaba.nacos.config.server.service.repository.extrnal;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.config.server.configuration.ConditionOnExternalStorage;
import com.alibaba.nacos.config.server.constant.Constants;
import com.alibaba.nacos.config.server.enums.FileTypeEnum;
import com.alibaba.nacos.config.server.model.ConfigAdvanceInfo;
import com.alibaba.nacos.config.server.model.ConfigAllInfo;
import com.alibaba.nacos.config.server.model.ConfigHistoryInfo;
import com.alibaba.nacos.config.server.model.ConfigInfo;
import com.alibaba.nacos.config.server.model.ConfigInfo4Beta;
import com.alibaba.nacos.config.server.model.ConfigInfo4Tag;
import com.alibaba.nacos.config.server.model.ConfigInfoAggr;
import com.alibaba.nacos.config.server.model.ConfigInfoBase;
import com.alibaba.nacos.config.server.model.ConfigInfoBetaWrapper;
import com.alibaba.nacos.config.server.model.ConfigInfoChanged;
import com.alibaba.nacos.config.server.model.ConfigInfoTagWrapper;
import com.alibaba.nacos.config.server.model.ConfigInfoWrapper;
import com.alibaba.nacos.config.server.model.ConfigKey;
import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.model.SameConfigPolicy;
import com.alibaba.nacos.config.server.model.SubInfo;
import com.alibaba.nacos.config.server.model.TenantInfo;
import com.alibaba.nacos.config.server.service.datasource.DataSourceService;
import com.alibaba.nacos.config.server.service.datasource.DynamicDataSource;
import com.alibaba.nacos.config.server.service.repository.PaginationHelper;
import com.alibaba.nacos.config.server.service.repository.PersistService;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.ConfigInfoAggrRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.ConfigInfoBetaRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.ConfigInfoRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.ConfigInfoTagRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.ConfigTagsRelationRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.HisConfigInfoRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.TenantInfoRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.dialect.DialectExistTable;
import com.alibaba.nacos.config.server.utils.LogUtil;
import com.alibaba.nacos.config.server.utils.ParamUtils;
import com.google.common.base.Joiner;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.nacos.config.server.service.repository.RowMapperManager.CONFIG_INFO_AGGR_ROW_MAPPER;
import static com.alibaba.nacos.config.server.service.repository.RowMapperManager.CONFIG_INFO_BASE_ROW_MAPPER;
import static com.alibaba.nacos.config.server.service.repository.RowMapperManager.CONFIG_INFO_ROW_MAPPER;
import static com.alibaba.nacos.config.server.service.repository.RowMapperManager.CONFIG_INFO_WRAPPER_ROW_MAPPER;
import static com.alibaba.nacos.config.server.service.repository.RowMapperManager.CONFIG_KEY_ROW_MAPPER;

/**
 * External Storage Persist Service.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 * @author klw
 */
@SuppressWarnings(value = {"PMD.MethodReturnWrapperTypeRule", "checkstyle:linelength"})
@Conditional(value = ConditionOnExternalStorage.class)
@Component
public class ExternalStoragePersistServiceImpl implements PersistService {
    
    @Autowired
    private ConfigInfoAggrRepository configInfoAggrRepository;
    
    @Autowired
    private ConfigInfoBetaRepository configInfoBetaRepository;
    
    @Autowired
    private ConfigInfoRepository configInfoRepository;
    
    @Autowired
    private ConfigInfoTagRepository configInfoTagRepository;
    
    @Autowired
    private ConfigTagsRelationRepository configTagsRelationRepository;
    
    @Autowired
    private HisConfigInfoRepository hisConfigInfoRepository;
    
    @Autowired
    private TenantInfoRepository tenantInfoRepository;
    
    @Autowired
    private DialectExistTable dialectExistTable;
    
    private DataSourceService dataSourceService;
    
    private static final String PATTERN_STR = "*";
    
    private static final int QUERY_LIMIT_SIZE = 50;
    
    protected JdbcTemplate jt;
    
    protected TransactionTemplate tjt;
    
    /**
     * constant variables.
     */
    public static final String SPOT = ".";
    
    /**
     * init datasource.
     */
    @PostConstruct
    public void init() {
        dataSourceService = DynamicDataSource.getInstance().getDataSource();
        
        jt = getJdbcTemplate();
        tjt = getTransactionTemplate();
    }
    
    public boolean checkMasterWritable() {
        return dataSourceService.checkMasterWritable();
    }
    
    public void setBasicDataSourceService(DataSourceService dataSourceService) {
        this.dataSourceService = dataSourceService;
    }
    
    public synchronized void reload() throws IOException {
        this.dataSourceService.reload();
    }
    
    /**
     * For unit testing.
     */
    public JdbcTemplate getJdbcTemplate() {
        return this.dataSourceService.getJdbcTemplate();
    }
    
    public TransactionTemplate getTransactionTemplate() {
        return this.dataSourceService.getTransactionTemplate();
    }
    
    @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
    public String getCurrentDBUrl() {
        return this.dataSourceService.getCurrentDbUrl();
    }
    
    @Override
    public <E> PaginationHelper<E> createPaginationHelper() {
        return new ExternalStoragePaginationHelperImpl<E>(jt);
    }
    
    // ----------------------- config_info table insert update delete
    
    @Override
    public void addConfigInfo(final String srcIp, final String srcUser, final ConfigInfo configInfo,
            final Timestamp time, final Map<String, Object> configAdvanceInfo, final boolean notify) {
        boolean result = tjt.execute(status -> {
            try {
                long configId = addConfigInfoAtomic(-1, srcIp, srcUser, configInfo, time, configAdvanceInfo);
                String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
                addConfigTagsRelation(configId, configTags, configInfo.getDataId(), configInfo.getGroup(),
                        configInfo.getTenant());
                insertConfigHistoryAtomic(0, configInfo, srcIp, srcUser, time, "I");
            } catch (CannotGetJdbcConnectionException e) {
                LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
                throw e;
            }
            return Boolean.TRUE;
        });
    }
    
    @Override
    public void addConfigInfo4Beta(ConfigInfo configInfo, String betaIps, String srcIp, String srcUser, Timestamp time,
            boolean notify) {
        configInfoBetaRepository.addConfigInfo4Beta(configInfo, betaIps, srcIp, srcUser, time, notify);
    }
    
    @Override
    public void addConfigInfo4Tag(ConfigInfo configInfo, String tag, String srcIp, String srcUser, Timestamp time,
            boolean notify) {
        configInfoTagRepository.addConfigInfo4Tag(configInfo, tag, srcIp, srcUser, time, notify);
    }
    
    @Override
    public void updateConfigInfo(final ConfigInfo configInfo, final String srcIp, final String srcUser,
            final Timestamp time, final Map<String, Object> configAdvanceInfo, final boolean notify) {
        boolean result = tjt.execute(status -> {
            try {
                ConfigInfo oldConfigInfo = findConfigInfo(configInfo.getDataId(), configInfo.getGroup(),
                        configInfo.getTenant());
                String appNameTmp = oldConfigInfo.getAppName();
                /*
                 If the appName passed by the user is not empty, use the persistent user's appName,
                 otherwise use db; when emptying appName, you need to pass an empty string
                 */
                if (configInfo.getAppName() == null) {
                    configInfo.setAppName(appNameTmp);
                }
                updateConfigInfoAtomic(configInfo, srcIp, srcUser, time, configAdvanceInfo);
                String configTags = configAdvanceInfo == null ? null : (String) configAdvanceInfo.get("config_tags");
                if (configTags != null) {
                    // delete all tags and then recreate
                    removeTagByIdAtomic(oldConfigInfo.getId());
                    addConfigTagsRelation(oldConfigInfo.getId(), configTags, configInfo.getDataId(),
                            configInfo.getGroup(), configInfo.getTenant());
                }
                insertConfigHistoryAtomic(oldConfigInfo.getId(), oldConfigInfo, srcIp, srcUser, time, "U");
            } catch (CannotGetJdbcConnectionException e) {
                LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
                throw e;
            }
            return Boolean.TRUE;
        });
    }
    
    @Override
    public void updateConfigInfo4Beta(ConfigInfo configInfo, String betaIps, String srcIp, String srcUser,
            Timestamp time, boolean notify) {
        configInfoBetaRepository.updateConfigInfo4Beta(configInfo, betaIps, srcIp, srcUser, time, notify);
    }
    
    @Override
    public void updateConfigInfo4Tag(ConfigInfo configInfo, String tag, String srcIp, String srcUser, Timestamp time,
            boolean notify) {
        configInfoTagRepository.updateConfigInfo4Tag(configInfo, tag, srcIp, srcUser, time, notify);
    }
    
    @Override
    public void insertOrUpdateBeta(final ConfigInfo configInfo, final String betaIps, final String srcIp,
            final String srcUser, final Timestamp time, final boolean notify) {
        try {
            addConfigInfo4Beta(configInfo, betaIps, srcIp, null, time, notify);
        } catch (DataIntegrityViolationException ive) { // Unique constraint conflict
            updateConfigInfo4Beta(configInfo, betaIps, srcIp, null, time, notify);
        }
    }
    
    @Override
    public void insertOrUpdateTag(final ConfigInfo configInfo, final String tag, final String srcIp,
            final String srcUser, final Timestamp time, final boolean notify) {
        try {
            addConfigInfo4Tag(configInfo, tag, srcIp, null, time, notify);
        } catch (DataIntegrityViolationException ive) { // Unique constraint conflict
            updateConfigInfo4Tag(configInfo, tag, srcIp, null, time, notify);
        }
    }
    
    @Override
    public void updateMd5(String dataId, String group, String tenant, String md5, Timestamp lastTime) {
        configInfoRepository.updateMd5(dataId, group, tenant, md5, lastTime);
    }
    
    @Override
    public void insertOrUpdate(String srcIp, String srcUser, ConfigInfo configInfo, Timestamp time,
            Map<String, Object> configAdvanceInfo) {
        insertOrUpdate(srcIp, srcUser, configInfo, time, configAdvanceInfo, true);
    }
    
    @Override
    public void insertOrUpdate(String srcIp, String srcUser, ConfigInfo configInfo, Timestamp time,
            Map<String, Object> configAdvanceInfo, boolean notify) {
        try {
            addConfigInfo(srcIp, srcUser, configInfo, time, configAdvanceInfo, notify);
        } catch (DataIntegrityViolationException ive) { // Unique constraint conflict
            updateConfigInfo(configInfo, srcIp, srcUser, time, configAdvanceInfo, notify);
        }
    }
    
    @Override
    public void insertOrUpdateSub(SubInfo subInfo) {
        try {
            addConfigSubAtomic(subInfo.getDataId(), subInfo.getGroup(), subInfo.getAppName(), subInfo.getDate());
        } catch (DataIntegrityViolationException ive) { // Unique constraint conflict
            updateConfigSubAtomic(subInfo.getDataId(), subInfo.getGroup(), subInfo.getAppName(), subInfo.getDate());
        }
    }
    
    @Override
    public void removeConfigInfo(final String dataId, final String group, final String tenant, final String srcIp,
            final String srcUser) {
        tjt.execute(new TransactionCallback<Boolean>() {
            final Timestamp time = new Timestamp(System.currentTimeMillis());
            
            @Override
            public Boolean doInTransaction(TransactionStatus status) {
                try {
                    ConfigInfo configInfo = findConfigInfo(dataId, group, tenant);
                    if (configInfo != null) {
                        removeConfigInfoAtomic(dataId, group, tenant, srcIp, srcUser);
                        removeTagByIdAtomic(configInfo.getId());
                        insertConfigHistoryAtomic(configInfo.getId(), configInfo, srcIp, srcUser, time, "D");
                    }
                } catch (CannotGetJdbcConnectionException e) {
                    LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
                    throw e;
                }
                return Boolean.TRUE;
            }
        });
    }
    
    @Override
    public List<ConfigInfo> removeConfigInfoByIds(final List<Long> ids, final String srcIp, final String srcUser) {
        if (CollectionUtils.isEmpty(ids)) {
            return null;
        }
        ids.removeAll(Collections.singleton(null));
        List<ConfigInfo> result = tjt.execute(new TransactionCallback<List<ConfigInfo>>() {
            final Timestamp time = new Timestamp(System.currentTimeMillis());
            
            @Override
            public List<ConfigInfo> doInTransaction(TransactionStatus status) {
                try {
                    String idsStr = Joiner.on(",").join(ids);
                    List<ConfigInfo> configInfoList = findConfigInfosByIds(idsStr);
                    if (!CollectionUtils.isEmpty(configInfoList)) {
                        removeConfigInfoByIdsAtomic(idsStr);
                        for (ConfigInfo configInfo : configInfoList) {
                            removeTagByIdAtomic(configInfo.getId());
                            insertConfigHistoryAtomic(configInfo.getId(), configInfo, srcIp, srcUser, time, "D");
                        }
                    }
                    return configInfoList;
                } catch (CannotGetJdbcConnectionException e) {
                    LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
                    throw e;
                }
            }
        });
        return result;
    }
    
    @Override
    public void removeConfigInfo4Beta(final String dataId, final String group, final String tenant) {
        configInfoBetaRepository.removeConfigInfo4Beta(dataId, group, tenant);
    }
    
    // ----------------------- config_aggr_info table insert update delete
    
    @Override
    public boolean addAggrConfigInfo(final String dataId, final String group, String tenant, final String datumId,
            String appName, final String content) {
        return configInfoAggrRepository.addAggrConfigInfo(dataId, group, tenant, datumId, appName, content);
    }
    
    @Override
    public void removeSingleAggrConfigInfo(final String dataId, final String group, final String tenant,
            final String datumId) {
        configInfoAggrRepository.removeSingleAggrConfigInfo(dataId, group, tenant, datumId);
    }
    
    @Override
    public void removeAggrConfigInfo(final String dataId, final String group, final String tenant) {
        configInfoAggrRepository.removeAggrConfigInfo(dataId, group, tenant);
    }
    
    @Override
    public boolean batchRemoveAggr(final String dataId, final String group, final String tenant,
            final List<String> datumList) {
        return configInfoAggrRepository.batchRemoveAggr(dataId, group, tenant, datumList);
    }
    
    @Override
    public void removeConfigHistory(final Timestamp startTime, final int limitSize) {
        hisConfigInfoRepository.removeConfigHistory(startTime, limitSize);
    }
    
    @Override
    public int findConfigHistoryCountByTime(final Timestamp startTime) {
        return hisConfigInfoRepository.findConfigHistoryCountByTime(startTime);
    }
    
    @Override
    public long findConfigMaxId() {
        return configInfoRepository.findConfigMaxId();
    }
    
    @Override
    public boolean batchPublishAggr(final String dataId, final String group, final String tenant,
            final Map<String, String> datumMap, final String appName) {
        try {
            Boolean isPublishOk = tjt.execute(new TransactionCallback<Boolean>() {
                @Override
                public Boolean doInTransaction(TransactionStatus status) {
                    for (Map.Entry<String, String> entry : datumMap.entrySet()) {
                        try {
                            if (!addAggrConfigInfo(dataId, group, tenant, entry.getKey(), appName, entry.getValue())) {
                                throw new TransactionSystemException("error in addAggrConfigInfo");
                            }
                        } catch (Throwable e) {
                            throw new TransactionSystemException("error in addAggrConfigInfo");
                        }
                    }
                    return Boolean.TRUE;
                }
            });
            if (isPublishOk == null) {
                return false;
            }
            return isPublishOk;
        } catch (TransactionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            return false;
        }
    }
    
    @Override
    public boolean replaceAggr(final String dataId, final String group, final String tenant,
            final Map<String, String> datumMap, final String appName) {
        return configInfoAggrRepository.replaceAggr(dataId, group, tenant, datumMap, appName);
    }
    
    @Deprecated
    @Override
    public List<ConfigInfo> findAllDataIdAndGroup() {
        String sql = "SELECT DISTINCT data_id, group_id FROM config_info";
        
        try {
            return jt.query(sql, new Object[] {}, CONFIG_INFO_ROW_MAPPER);
        } catch (EmptyResultDataAccessException e) {
            return Collections.emptyList();
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        } catch (Exception e) {
            LogUtil.FATAL_LOG.error("[db-other-error]" + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public ConfigInfo4Beta findConfigInfo4Beta(final String dataId, final String group, final String tenant) {
        return configInfoBetaRepository.findConfigInfo4Beta(dataId, group, tenant);
    }
    
    @Override
    public ConfigInfo4Tag findConfigInfo4Tag(final String dataId, final String group, final String tenant,
            final String tag) {
        return configInfoTagRepository.findConfigInfo4Tag(dataId, group, tenant, tag);
    }
    
    @Override
    public ConfigInfo findConfigInfoApp(final String dataId, final String group, final String tenant,
            final String appName) {
        return configInfoRepository.findConfigInfoApp(dataId, group, tenant, appName);
    }
    
    @Override
    public ConfigInfo findConfigInfoAdvanceInfo(final String dataId, final String group, final String tenant,
            final Map<String, Object> configAdvanceInfo) {
        return configInfoRepository.findConfigInfoAdvanceInfo(dataId, group, tenant, configAdvanceInfo);
    }
    
    @Override
    public ConfigInfoBase findConfigInfoBase(final String dataId, final String group) {
        return configInfoRepository.findConfigInfoBase(dataId, group);
    }
    
    @Override
    public ConfigInfo findConfigInfo(long id) {
        return configInfoRepository.findConfigInfo(id);
    }
    
    @Override
    public ConfigInfo findConfigInfo(final String dataId, final String group, final String tenant) {
        return configInfoRepository.findConfigInfo(dataId, group, tenant);
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoByDataId(final int pageNo, final int pageSize, final String dataId,
            final String tenant) {
        return configInfoRepository.findConfigInfoByDataId(pageNo, pageSize, dataId, tenant);
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoByDataIdAndApp(final int pageNo, final int pageSize, final String dataId,
            final String tenant, final String appName) {
        return configInfoRepository.findConfigInfoByDataIdAndApp(pageNo, pageSize, dataId, tenant, appName);
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoByDataIdAndAdvance(final int pageNo, final int pageSize, final String dataId,
            final String tenant, final Map<String, Object> configAdvanceInfo) {
        return configInfoRepository
                .findConfigInfoByDataIdAndAdvance(pageNo, pageSize, dataId, tenant, configAdvanceInfo);
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfo4Page(final int pageNo, final int pageSize, final String dataId,
            final String group, final String tenant, final Map<String, Object> configAdvanceInfo) {
        return configInfoRepository.findConfigInfo4Page(pageNo, pageSize, dataId, group, tenant, configAdvanceInfo);
    }
    
    @Override
    public Page<ConfigInfoBase> findConfigInfoBaseByDataId(final int pageNo, final int pageSize, final String dataId) {
        return configInfoRepository.findConfigInfoBaseByDataId(pageNo, pageSize, dataId);
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoByGroup(final int pageNo, final int pageSize, final String group,
            final String tenant) {
        return configInfoRepository.findConfigInfoByGroup(pageNo, pageSize, group, tenant);
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoByGroupAndApp(final int pageNo, final int pageSize, final String group,
            final String tenant, final String appName) {
        return configInfoRepository.findConfigInfoByGroupAndApp(pageNo, pageSize, group, tenant, appName);
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoByGroupAndAdvance(final int pageNo, final int pageSize, final String group,
            final String tenant, final Map<String, Object> configAdvanceInfo) {
        return configInfoRepository.findConfigInfoByGroupAndAdvance(pageNo, pageSize, group, tenant, configAdvanceInfo);
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoByApp(final int pageNo, final int pageSize, final String tenant,
            final String appName) {
        return configInfoRepository.findConfigInfoByApp(pageNo, pageSize, tenant, appName);
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoByAdvance(final int pageNo, final int pageSize, final String tenant,
            final Map<String, Object> configAdvanceInfo) {
        return configInfoRepository.findConfigInfoByAdvance(pageNo, pageSize, tenant, configAdvanceInfo);
    }
    
    @Override
    
    public Page<ConfigInfoBase> findConfigInfoBaseByGroup(final int pageNo, final int pageSize, final String group) {
        return configInfoRepository.findConfigInfoBaseByGroup(pageNo, pageSize, group);
    }
    
    @Override
    public int configInfoCount() {
        return configInfoRepository.configInfoCount();
    }
    
    @Override
    public int configInfoCount(String tenant) {
        return configInfoRepository.configInfoCount(tenant);
    }
    
    @Override
    public int configInfoBetaCount() {
        return configInfoBetaRepository.configInfoBetaCount();
    }
    
    @Override
    public int configInfoTagCount() {
        return configInfoTagRepository.configInfoTagCount();
    }
    
    @Override
    public List<String> getTenantIdList(int page, int pageSize) {
        return configInfoRepository.getTenantIdList(page, pageSize);
    }
    
    @Override
    public List<String> getGroupIdList(int page, int pageSize) {
        return configInfoRepository.getGroupIdList(page, pageSize);
    }
    
    @Override
    public int aggrConfigInfoCount(String dataId, String group, String tenant) {
        return configInfoAggrRepository.aggrConfigInfoCount(dataId, group, tenant);
    }
    
    @Override
    public int aggrConfigInfoCount(String dataId, String group, String tenant, List<String> datumIds, boolean isIn) {
        return configInfoAggrRepository.aggrConfigInfoCount(dataId, group, tenant, datumIds, isIn);
    }
    
    @Override
    public int aggrConfigInfoCountIn(String dataId, String group, String tenant, List<String> datumIds) {
        return aggrConfigInfoCount(dataId, group, tenant, datumIds, true);
    }
    
    @Override
    public int aggrConfigInfoCountNotIn(String dataId, String group, String tenant, List<String> datumIds) {
        return aggrConfigInfoCount(dataId, group, tenant, datumIds, false);
    }
    
    @Override
    public Page<ConfigInfo> findAllConfigInfo(final int pageNo, final int pageSize, final String tenant) {
        return configInfoRepository.findAllConfigInfo(pageNo, pageSize, tenant);
    }
    
    @Override
    public Page<ConfigKey> findAllConfigKey(final int pageNo, final int pageSize, final String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        String select = " SELECT data_id,group_id,app_name  FROM ( "
                + " SELECT id FROM config_info WHERE tenant_id LIKE ? ORDER BY id LIMIT ?, ?  )"
                + " g, config_info t WHERE g.id = t.id  ";
        
        final int totalCount = configInfoCount(tenant);
        int pageCount = totalCount / pageSize;
        if (totalCount > pageSize * pageCount) {
            pageCount++;
        }
        
        if (pageNo > pageCount) {
            return null;
        }
        
        final Page<ConfigKey> page = new Page<ConfigKey>();
        page.setPageNumber(pageNo);
        page.setPagesAvailable(pageCount);
        page.setTotalCount(totalCount);
        
        try {
            List<ConfigKey> result = jt
                    .query(select, new Object[] {generateLikeArgument(tenantTmp), (pageNo - 1) * pageSize, pageSize},
                            // new Object[0],
                            CONFIG_KEY_ROW_MAPPER);
            
            for (ConfigKey item : result) {
                page.getPageItems().add(item);
            }
            return page;
        } catch (EmptyResultDataAccessException e) {
            return page;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    @Deprecated
    public Page<ConfigInfoBase> findAllConfigInfoBase(final int pageNo, final int pageSize) {
        String sqlCountRows = "SELECT COUNT(*) FROM config_info";
        String sqlFetchRows = " SELECT t.id,data_id,group_id,content,md5"
                + " FROM ( SELECT id FROM config_info ORDER BY id LIMIT ?,?  ) "
                + " g, config_info t  WHERE g.id = t.id ";
        
        PaginationHelper<ConfigInfoBase> helper = createPaginationHelper();
        try {
            return helper.fetchPageLimit(sqlCountRows, sqlFetchRows, new Object[] {(pageNo - 1) * pageSize, pageSize},
                    pageNo, pageSize, CONFIG_INFO_BASE_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public Page<ConfigInfoWrapper> findAllConfigInfoForDumpAll(final int pageNo, final int pageSize) {
        return configInfoRepository.findAllConfigInfoForDumpAll(pageNo, pageSize);
    }
    
    @Override
    public Page<ConfigInfoWrapper> findAllConfigInfoFragment(final long lastMaxId, final int pageSize) {
        return configInfoRepository.findAllConfigInfoFragment(lastMaxId, pageSize);
    }
    
    @Override
    public Page<ConfigInfoBetaWrapper> findAllConfigInfoBetaForDumpAll(final int pageNo, final int pageSize) {
        return configInfoBetaRepository.findAllConfigInfoBetaForDumpAll(pageNo, pageSize);
    }
    
    @Override
    public Page<ConfigInfoTagWrapper> findAllConfigInfoTagForDumpAll(final int pageNo, final int pageSize) {
        return configInfoTagRepository.findAllConfigInfoTagForDumpAll(pageNo, pageSize);
    }
    
    @Override
    public List<ConfigInfo> findConfigInfoByBatch(final List<String> dataIds, final String group, final String tenant,
            int subQueryLimit) {
        // assert dataids group not null
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        // if dataids empty return empty list
        if (CollectionUtils.isEmpty(dataIds)) {
            return Collections.emptyList();
        }
        
        // Batch query limit
        // The number of in is controlled within 100, the shorter the length of the SQL statement, the better
        if (subQueryLimit > QUERY_LIMIT_SIZE) {
            subQueryLimit = 50;
        }
        List<ConfigInfo> result = new ArrayList<ConfigInfo>(dataIds.size());
        
        String sqlStart = "select data_id, group_id, tenant_id, app_name, content from config_info where group_id = ? and tenant_id = ? and data_id in (";
        String sqlEnd = ")";
        StringBuilder subQuerySql = new StringBuilder();
        
        for (int i = 0; i < dataIds.size(); i += subQueryLimit) {
            // dataids
            List<String> params = new ArrayList<String>(
                    dataIds.subList(i, i + subQueryLimit < dataIds.size() ? i + subQueryLimit : dataIds.size()));
            
            for (int j = 0; j < params.size(); j++) {
                subQuerySql.append("?");
                if (j != params.size() - 1) {
                    subQuerySql.append(",");
                }
            }
            
            // group
            params.add(0, group);
            params.add(1, tenantTmp);
            
            List<ConfigInfo> r = this.jt
                    .query(sqlStart + subQuerySql.toString() + sqlEnd, params.toArray(), CONFIG_INFO_ROW_MAPPER);
            
            // assert not null
            if (r != null && r.size() > 0) {
                result.addAll(r);
            }
        }
        return result;
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoLike(final int pageNo, final int pageSize, final String dataId,
            final String group, final String tenant, final String appName, final String content) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        if (StringUtils.isBlank(dataId) && StringUtils.isBlank(group)) {
            if (StringUtils.isBlank(appName)) {
                return this.findAllConfigInfo(pageNo, pageSize, tenantTmp);
            } else {
                return this.findConfigInfoByApp(pageNo, pageSize, tenantTmp, appName);
            }
        }
        
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        
        String sqlCountRows = "select count(*) from config_info where ";
        String sqlFetchRows = "select ID,data_id,group_id,tenant_id,app_name,content from config_info where ";
        String where = " 1=1 ";
        List<String> params = new ArrayList<String>();
        
        if (!StringUtils.isBlank(dataId)) {
            where += " and data_id like ? ";
            params.add(generateLikeArgument(dataId));
        }
        if (!StringUtils.isBlank(group)) {
            where += " and group_id like ? ";
            params.add(generateLikeArgument(group));
        }
        
        where += " and tenant_id like ? ";
        params.add(generateLikeArgument(tenantTmp));
        
        if (!StringUtils.isBlank(appName)) {
            where += " and app_name = ? ";
            params.add(appName);
        }
        if (!StringUtils.isBlank(content)) {
            where += " and content like ? ";
            params.add(generateLikeArgument(content));
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
    public Page<ConfigInfo> findConfigInfoLike(final int pageNo, final int pageSize, final ConfigKey[] configKeys,
            final boolean blacklist) {
        String sqlCountRows = "select count(*) from config_info where ";
        String sqlFetchRows = "select ID,data_id,group_id,tenant_id,app_name,content from config_info where ";
        StringBuilder where = new StringBuilder(" 1=1 ");
        // Whitelist, please leave the synchronization condition empty, there is no configuration that meets the conditions
        if (configKeys.length == 0 && blacklist == false) {
            Page<ConfigInfo> page = new Page<ConfigInfo>();
            page.setTotalCount(0);
            return page;
        }
        PaginationHelper<ConfigInfo> helper = createPaginationHelper();
        List<String> params = new ArrayList<String>();
        boolean isFirst = true;
        for (ConfigKey configInfo : configKeys) {
            String dataId = configInfo.getDataId();
            String group = configInfo.getGroup();
            String appName = configInfo.getAppName();
            
            if (StringUtils.isBlank(dataId) && StringUtils.isBlank(group) && StringUtils.isBlank(appName)) {
                break;
            }
            
            if (blacklist) {
                if (isFirst) {
                    isFirst = false;
                    where.append(" and ");
                } else {
                    where.append(" and ");
                }
                
                where.append("(");
                boolean isFirstSub = true;
                if (!StringUtils.isBlank(dataId)) {
                    where.append(" data_id not like ? ");
                    params.add(generateLikeArgument(dataId));
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(group)) {
                    if (!isFirstSub) {
                        where.append(" or ");
                    }
                    where.append(" group_id not like ? ");
                    params.add(generateLikeArgument(group));
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(appName)) {
                    if (!isFirstSub) {
                        where.append(" or ");
                    }
                    where.append(" app_name != ? ");
                    params.add(appName);
                    isFirstSub = false;
                }
                where.append(") ");
            } else {
                if (isFirst) {
                    isFirst = false;
                    where.append(" and ");
                } else {
                    where.append(" or ");
                }
                where.append("(");
                boolean isFirstSub = true;
                if (!StringUtils.isBlank(dataId)) {
                    where.append(" data_id like ? ");
                    params.add(generateLikeArgument(dataId));
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(group)) {
                    if (!isFirstSub) {
                        where.append(" and ");
                    }
                    where.append(" group_id like ? ");
                    params.add(generateLikeArgument(group));
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(appName)) {
                    if (!isFirstSub) {
                        where.append(" and ");
                    }
                    where.append(" app_name = ? ");
                    params.add(appName);
                    isFirstSub = false;
                }
                where.append(") ");
            }
        }
        
        try {
            return helper.fetchPage(sqlCountRows + where.toString(), sqlFetchRows + where.toString(), params.toArray(),
                    pageNo, pageSize, CONFIG_INFO_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public Page<ConfigInfo> findConfigInfoLike4Page(final int pageNo, final int pageSize, final String dataId,
            final String group, final String tenant, final Map<String, Object> configAdvanceInfo) {
        return configInfoRepository.findConfigInfoLike4Page(pageNo, pageSize, dataId, group, tenant, configAdvanceInfo);
    }
    
    @Override
    public Page<ConfigInfoBase> findConfigInfoBaseLike(final int pageNo, final int pageSize, final String dataId,
            final String group, final String content) throws IOException {
        if (StringUtils.isBlank(dataId) && StringUtils.isBlank(group)) {
            throw new IOException("invalid param");
        }
        
        PaginationHelper<ConfigInfoBase> helper = createPaginationHelper();
        
        String sqlCountRows = "select count(*) from config_info where ";
        String sqlFetchRows = "select ID,data_id,group_id,tenant_id,content from config_info where ";
        String where = " 1=1 and tenant_id='' ";
        List<String> params = new ArrayList<String>();
        
        if (!StringUtils.isBlank(dataId)) {
            where += " and data_id like ? ";
            params.add(generateLikeArgument(dataId));
        }
        if (!StringUtils.isBlank(group)) {
            where += " and group_id like ? ";
            params.add(generateLikeArgument(group));
        }
        if (!StringUtils.isBlank(content)) {
            where += " and content like ? ";
            params.add(generateLikeArgument(content));
        }
        
        try {
            return helper.fetchPage(sqlCountRows + where, sqlFetchRows + where, params.toArray(), pageNo, pageSize,
                    CONFIG_INFO_BASE_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public ConfigInfoAggr findSingleConfigInfoAggr(String dataId, String group, String tenant, String datumId) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        String sql = "SELECT id,data_id,group_id,tenant_id,datum_id,app_name,content FROM config_info_aggr WHERE data_id=? AND group_id=? AND tenant_id=? AND datum_id=?";
        
        try {
            return this.jt
                    .queryForObject(sql, new Object[] {dataId, group, tenantTmp, datumId}, CONFIG_INFO_AGGR_ROW_MAPPER);
        } catch (EmptyResultDataAccessException e) {
            // EmptyResultDataAccessException, indicating that the data does not exist, returns null
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        } catch (Exception e) {
            LogUtil.FATAL_LOG.error("[db-other-error]" + e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    public List<ConfigInfoAggr> findConfigInfoAggr(String dataId, String group, String tenant) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        String sql = "SELECT data_id,group_id,tenant_id,datum_id,app_name,content FROM config_info_aggr WHERE data_id=? AND group_id=? AND tenant_id=? ORDER BY datum_id";
        
        try {
            return this.jt.query(sql, new Object[] {dataId, group, tenantTmp}, CONFIG_INFO_AGGR_ROW_MAPPER);
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
    public Page<ConfigInfoAggr> findConfigInfoAggrByPage(String dataId, String group, String tenant, final int pageNo,
            final int pageSize) {
        return configInfoAggrRepository.findConfigInfoAggrByPage(dataId, group, tenant, pageNo, pageSize);
    }
    
    @Override
    public Page<ConfigInfoAggr> findConfigInfoAggrLike(final int pageNo, final int pageSize, ConfigKey[] configKeys,
            boolean blacklist) {
        
        String sqlCountRows = "select count(*) from config_info_aggr where ";
        String sqlFetchRows = "select data_id,group_id,tenant_id,datum_id,app_name,content from config_info_aggr where ";
        StringBuilder where = new StringBuilder(" 1=1 ");
        // Whitelist, please leave the synchronization condition empty, there is no configuration that meets the conditions
        if (configKeys.length == 0 && blacklist == false) {
            Page<ConfigInfoAggr> page = new Page<ConfigInfoAggr>();
            page.setTotalCount(0);
            return page;
        }
        PaginationHelper<ConfigInfoAggr> helper = createPaginationHelper();
        List<String> params = new ArrayList<String>();
        boolean isFirst = true;
        
        for (ConfigKey configInfoAggr : configKeys) {
            String dataId = configInfoAggr.getDataId();
            String group = configInfoAggr.getGroup();
            String appName = configInfoAggr.getAppName();
            if (StringUtils.isBlank(dataId) && StringUtils.isBlank(group) && StringUtils.isBlank(appName)) {
                break;
            }
            if (blacklist) {
                if (isFirst) {
                    isFirst = false;
                    where.append(" and ");
                } else {
                    where.append(" and ");
                }
                
                where.append("(");
                boolean isFirstSub = true;
                if (!StringUtils.isBlank(dataId)) {
                    where.append(" data_id not like ? ");
                    params.add(generateLikeArgument(dataId));
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(group)) {
                    if (!isFirstSub) {
                        where.append(" or ");
                    }
                    where.append(" group_id not like ? ");
                    params.add(generateLikeArgument(group));
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(appName)) {
                    if (!isFirstSub) {
                        where.append(" or ");
                    }
                    where.append(" app_name != ? ");
                    params.add(appName);
                    isFirstSub = false;
                }
                where.append(") ");
            } else {
                if (isFirst) {
                    isFirst = false;
                    where.append(" and ");
                } else {
                    where.append(" or ");
                }
                where.append("(");
                boolean isFirstSub = true;
                if (!StringUtils.isBlank(dataId)) {
                    where.append(" data_id like ? ");
                    params.add(generateLikeArgument(dataId));
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(group)) {
                    if (!isFirstSub) {
                        where.append(" and ");
                    }
                    where.append(" group_id like ? ");
                    params.add(generateLikeArgument(group));
                    isFirstSub = false;
                }
                if (!StringUtils.isBlank(appName)) {
                    if (!isFirstSub) {
                        where.append(" and ");
                    }
                    where.append(" app_name = ? ");
                    params.add(appName);
                    isFirstSub = false;
                }
                where.append(") ");
            }
        }
        
        try {
            Page<ConfigInfoAggr> result = helper
                    .fetchPage(sqlCountRows + where.toString(), sqlFetchRows + where.toString(), params.toArray(),
                            pageNo, pageSize, CONFIG_INFO_AGGR_ROW_MAPPER);
            return result;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public List<ConfigInfoChanged> findAllAggrGroup() {
        return configInfoAggrRepository.findAllAggrGroup();
    }
    
    @Override
    public List<String> findDatumIdByContent(String dataId, String groupId, String content) {
        String sql = "SELECT datum_id FROM config_info_aggr WHERE data_id = ? AND group_id = ? AND content = ? ";
        
        try {
            return this.jt.queryForList(sql, new Object[] {dataId, groupId, content}, String.class);
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (IncorrectResultSizeDataAccessException e) {
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public List<ConfigInfoWrapper> findChangeConfig(final Timestamp startTime, final Timestamp endTime) {
        List<Map<String, Object>> list = configInfoRepository.findChangeConfig(startTime, endTime);
        return convertChangeConfig(list);
    }
    
    @Override
    public Page<ConfigInfoWrapper> findChangeConfig(final String dataId, final String group, final String tenant,
            final String appName, final Timestamp startTime, final Timestamp endTime, final int pageNo,
            final int pageSize, final long lastMaxId) {
        String tenantTmp = StringUtils.isBlank(tenant) ? StringUtils.EMPTY : tenant;
        String sqlCountRows = "select count(*) from config_info where ";
        String sqlFetchRows = "select id,data_id,group_id,tenant_id,app_name,content,type,md5,gmt_modified from config_info where ";
        String where = " 1=1 ";
        List<Object> params = new ArrayList<Object>();
        
        if (!StringUtils.isBlank(dataId)) {
            where += " and data_id like ? ";
            params.add(generateLikeArgument(dataId));
        }
        if (!StringUtils.isBlank(group)) {
            where += " and group_id like ? ";
            params.add(generateLikeArgument(group));
        }
        
        if (!StringUtils.isBlank(tenantTmp)) {
            where += " and tenant_id = ? ";
            params.add(tenantTmp);
        }
        
        if (!StringUtils.isBlank(appName)) {
            where += " and app_name = ? ";
            params.add(appName);
        }
        if (startTime != null) {
            where += " and gmt_modified >=? ";
            params.add(startTime);
        }
        if (endTime != null) {
            where += " and gmt_modified <=? ";
            params.add(endTime);
        }
        
        PaginationHelper<ConfigInfoWrapper> helper = createPaginationHelper();
        try {
            return helper.fetchPage(sqlCountRows + where, sqlFetchRows + where, params.toArray(), pageNo, pageSize,
                    lastMaxId, CONFIG_INFO_WRAPPER_ROW_MAPPER);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public List<ConfigInfo> findDeletedConfig(final Timestamp startTime, final Timestamp endTime) {
        List<Map<String, Object>> list = configInfoRepository.findDeletedConfig(startTime, endTime);
        return convertDeletedConfig(list);
    }
    
    @Override
    public long addConfigInfoAtomic(final long configId, final String srcIp, final String srcUser,
            final ConfigInfo configInfo, final Timestamp time, Map<String, Object> configAdvanceInfo) {
        return configInfoRepository.addConfigInfoAtomic(configId, srcIp, srcUser, configInfo, time, configAdvanceInfo);
    }
    
    @Override
    public void addConfigTagsRelation(long configId, String configTags, String dataId, String group, String tenant) {
        if (StringUtils.isNotBlank(configTags)) {
            String[] tagArr = configTags.split(",");
            for (int i = 0; i < tagArr.length; i++) {
                addConfigTagRelationAtomic(configId, tagArr[i], dataId, group, tenant);
            }
        }
    }
    
    @Override
    public void addConfigTagRelationAtomic(long configId, String tagName, String dataId, String group, String tenant) {
        configTagsRelationRepository.addConfigTagRelationAtomic(configId, tagName, dataId, group, tenant);
    }
    
    @Override
    public void removeTagByIdAtomic(long id) {
        configTagsRelationRepository.removeTagByIdAtomic(id);
    }
    
    @Override
    public List<String> getConfigTagsByTenant(String tenant) {
        String sql = "SELECT tag_name FROM config_tags_relation WHERE tenant_id = ? ";
        try {
            return jt.queryForList(sql, new Object[] {tenant}, String.class);
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (IncorrectResultSizeDataAccessException e) {
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public List<String> selectTagByConfig(String dataId, String group, String tenant) {
        return configTagsRelationRepository.selectTagByConfig(dataId, group, tenant);
    }
    
    @Override
    public void removeConfigInfoAtomic(final String dataId, final String group, final String tenant, final String srcIp,
            final String srcUser) {
        configInfoRepository.removeConfigInfoAtomic(dataId, group, tenant, srcIp, srcUser);
    }
    
    @Override
    public void removeConfigInfoByIdsAtomic(final String ids) {
        configInfoRepository.removeConfigInfoByIdsAtomic(ids);
    }
    
    @Override
    public void removeConfigInfoTag(final String dataId, final String group, final String tenant, final String tag,
            final String srcIp, final String srcUser) {
        configInfoTagRepository.removeConfigInfoTag(dataId, group, tenant, tag, srcIp, srcUser);
    }
    
    @Override
    public void updateConfigInfoAtomic(final ConfigInfo configInfo, final String srcIp, final String srcUser,
            final Timestamp time, Map<String, Object> configAdvanceInfo) {
        configInfoRepository.updateConfigInfoAtomic(configInfo, srcIp, srcUser, time, configAdvanceInfo);
    }
    
    @Override
    public List<ConfigInfo> findConfigInfosByIds(final String ids) {
        return configInfoRepository.findConfigInfosByIds(ids);
    }
    
    @Override
    public ConfigAdvanceInfo findConfigAdvanceInfo(final String dataId, final String group, final String tenant) {
        try {
            List<String> configTagList = this.selectTagByConfig(dataId, group, tenant);
            ConfigAdvanceInfo configAdvance = configInfoRepository.findConfigAdvanceInfo(dataId, group, tenant);
            if (configTagList != null && !configTagList.isEmpty()) {
                StringBuilder configTagsTmp = new StringBuilder();
                for (String configTag : configTagList) {
                    if (configTagsTmp.length() == 0) {
                        configTagsTmp.append(configTag);
                    } else {
                        configTagsTmp.append(",").append(configTag);
                    }
                }
                configAdvance.setConfigTags(configTagsTmp.toString());
            }
            return configAdvance;
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public ConfigAllInfo findConfigAllInfo(final String dataId, final String group, final String tenant) {
        try {
            List<String> configTagList = this.selectTagByConfig(dataId, group, tenant);
            ConfigAllInfo configAdvance = configInfoRepository.findConfigAllInfo(dataId, group, tenant);
            if (configTagList != null && !configTagList.isEmpty()) {
                StringBuilder configTagsTmp = new StringBuilder();
                for (String configTag : configTagList) {
                    if (configTagsTmp.length() == 0) {
                        configTagsTmp.append(configTag);
                    } else {
                        configTagsTmp.append(",").append(configTag);
                    }
                }
                configAdvance.setConfigTags(configTagsTmp.toString());
            }
            return configAdvance;
        } catch (EmptyResultDataAccessException e) { // Indicates that the data does not exist, returns null
            return null;
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public void insertConfigHistoryAtomic(long id, ConfigInfo configInfo, String srcIp, String srcUser,
            final Timestamp time, String ops) {
        hisConfigInfoRepository.insertConfigHistoryAtomic(id, configInfo, srcIp, srcUser, time, ops);
    }
    
    @Override
    public Page<ConfigHistoryInfo> findConfigHistory(String dataId, String group, String tenant, int pageNo,
            int pageSize) {
        return hisConfigInfoRepository.findConfigHistory(dataId, group, tenant, pageNo, pageSize);
    }
    
    @Override
    public void addConfigSubAtomic(final String dataId, final String group, final String appName,
            final Timestamp date) {
        final String appNameTmp = appName == null ? "" : appName;
        try {
            jt.update(
                    "INSERT INTO app_configdata_relation_subs(data_id,group_id,app_name,gmt_modified) VALUES(?,?,?,?)",
                    dataId, group, appNameTmp, date);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public void updateConfigSubAtomic(final String dataId, final String group, final String appName,
            final Timestamp time) {
        final String appNameTmp = appName == null ? "" : appName;
        try {
            jt.update(
                    "UPDATE app_configdata_relation_subs SET gmt_modified=? WHERE data_id=? AND group_id=? AND app_name=?",
                    time, dataId, group, appNameTmp);
        } catch (CannotGetJdbcConnectionException e) {
            LogUtil.FATAL_LOG.error("[db-error] " + e.toString(), e);
            throw e;
        }
    }
    
    @Override
    public ConfigHistoryInfo detailConfigHistory(Long nid) {
        return hisConfigInfoRepository.detailConfigHistory(nid);
    }
    
    @Override
    public ConfigHistoryInfo detailPreviousConfigHistory(Long id) {
        return hisConfigInfoRepository.detailPreviousConfigHistory(id);
    }
    
    @Override
    public void insertTenantInfoAtomic(String kp, String tenantId, String tenantName, String tenantDesc,
            String createResoure, final long time) {
        tenantInfoRepository.insertTenantInfoAtomic(kp, tenantId, tenantName, tenantDesc, createResoure, time);
    }
    
    @Override
    public void updateTenantNameAtomic(String kp, String tenantId, String tenantName, String tenantDesc) {
        tenantInfoRepository.updateTenantNameAtomic(kp, tenantId, tenantName, tenantDesc);
    }
    
    @Override
    public List<TenantInfo> findTenantByKp(String kp) {
        return tenantInfoRepository.findTenantByKp(kp);
    }
    
    @Override
    public TenantInfo findTenantByKp(String kp, String tenantId) {
        return tenantInfoRepository.findTenantByKp(kp, tenantId);
    }
    
    @Override
    public void removeTenantInfoAtomic(final String kp, final String tenantId) {
        tenantInfoRepository.removeTenantInfoAtomic(kp, tenantId);
    }
    
    @Override
    public List<ConfigInfo> convertDeletedConfig(List<Map<String, Object>> list) {
        List<ConfigInfo> configs = new ArrayList<ConfigInfo>();
        for (Map<String, Object> map : list) {
            String dataId = (String) map.get("data_id");
            String group = (String) map.get("group_id");
            String tenant = (String) map.get("tenant_id");
            ConfigInfo config = new ConfigInfo();
            config.setDataId(dataId);
            config.setGroup(group);
            config.setTenant(tenant);
            configs.add(config);
        }
        return configs;
    }
    
    @Override
    public List<ConfigInfoWrapper> convertChangeConfig(List<Map<String, Object>> list) {
        List<ConfigInfoWrapper> configs = new ArrayList<ConfigInfoWrapper>();
        for (Map<String, Object> map : list) {
            String dataId = (String) map.get("data_id");
            String group = (String) map.get("group_id");
            String tenant = (String) map.get("tenant_id");
            String content = (String) map.get("content");
            long mTime = ((Timestamp) map.get("gmt_modified")).getTime();
            ConfigInfoWrapper config = new ConfigInfoWrapper();
            config.setDataId(dataId);
            config.setGroup(group);
            config.setTenant(tenant);
            config.setContent(content);
            config.setLastModified(mTime);
            configs.add(config);
        }
        return configs;
    }
    
    @Override
    public List<ConfigInfoWrapper> listAllGroupKeyMd5() {
        final int pageSize = 10000;
        int totalCount = configInfoCount();
        int pageCount = (int) Math.ceil(totalCount * 1.0 / pageSize);
        List<ConfigInfoWrapper> allConfigInfo = new ArrayList<ConfigInfoWrapper>();
        for (int pageNo = 1; pageNo <= pageCount; pageNo++) {
            List<ConfigInfoWrapper> configInfoList = listGroupKeyMd5ByPage(pageNo, pageSize);
            allConfigInfo.addAll(configInfoList);
        }
        return allConfigInfo;
    }
    
    @Override
    public List<ConfigInfoWrapper> listGroupKeyMd5ByPage(int pageNo, int pageSize) {
        return configInfoRepository.listGroupKeyMd5ByPage(pageNo, pageSize);
    }
    
    @Override
    public String generateLikeArgument(String s) {
        String fuzzySearchSign = "\\*";
        String sqlLikePercentSign = "%";
        if (s.contains(PATTERN_STR)) {
            return s.replaceAll(fuzzySearchSign, sqlLikePercentSign);
        } else {
            return s;
        }
    }
    
    @Override
    public ConfigInfoWrapper queryConfigInfo(final String dataId, final String group, final String tenant) {
        return configInfoRepository.queryConfigInfo(dataId, group, tenant);
    }
    
    @Override
    public boolean isExistTable(String tableName) {
        String existTableSql = dialectExistTable.isExistTable(tableName);
        try {
            jt.queryForObject(existTableSql, Integer.class);
            return true;
        } catch (Throwable e) {
            return false;
        }
    }
    
    @Override
    public Boolean completeMd5() {
        LogUtil.DEFAULT_LOG.info("[start completeMd5]");
        int perPageSize = 1000;
        int rowCount = configInfoCount();
        int pageCount = (int) Math.ceil(rowCount * 1.0 / perPageSize);
        int actualRowCount = 0;
        for (int pageNo = 1; pageNo <= pageCount; pageNo++) {
            Page<ConfigInfoWrapper> page = findAllConfigInfoForDumpAll(pageNo, perPageSize);
            if (page != null) {
                for (ConfigInfoWrapper cf : page.getPageItems()) {
                    String md5InDb = cf.getMd5();
                    final String content = cf.getContent();
                    final String tenant = cf.getTenant();
                    final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
                    if (StringUtils.isBlank(md5InDb)) {
                        try {
                            updateMd5(cf.getDataId(), cf.getGroup(), tenant, md5, new Timestamp(cf.getLastModified()));
                        } catch (Exception e) {
                            LogUtil.DEFAULT_LOG.error("[completeMd5-error] datId:{} group:{} lastModified:{}",
                                    new Object[] {cf.getDataId(), cf.getGroup(), new Timestamp(cf.getLastModified())});
                        }
                    } else {
                        if (!md5InDb.equals(md5)) {
                            try {
                                updateMd5(cf.getDataId(), cf.getGroup(), tenant, md5,
                                        new Timestamp(cf.getLastModified()));
                            } catch (Exception e) {
                                LogUtil.DEFAULT_LOG.error("[completeMd5-error] datId:{} group:{} lastModified:{}",
                                        new Object[] {cf.getDataId(), cf.getGroup(),
                                                new Timestamp(cf.getLastModified())});
                            }
                        }
                    }
                }
                
                actualRowCount += page.getPageItems().size();
                LogUtil.DEFAULT_LOG.info("[completeMd5] {} / {}", actualRowCount, rowCount);
            }
        }
        return true;
    }
    
    @Override
    public List<ConfigAllInfo> findAllConfigInfo4Export(final String dataId, final String group, final String tenant,
            final String appName, final List<Long> ids) {
        return configInfoRepository.findAllConfigInfo4Export(dataId, group, tenant, appName, ids);
    }
    
    @Override
    public Map<String, Object> batchInsertOrUpdate(List<ConfigAllInfo> configInfoList, String srcUser, String srcIp,
            Map<String, Object> configAdvanceInfo, Timestamp time, boolean notify, SameConfigPolicy policy)
            throws NacosException {
        int succCount = 0;
        int skipCount = 0;
        List<Map<String, String>> failData = null;
        List<Map<String, String>> skipData = null;
        
        for (int i = 0; i < configInfoList.size(); i++) {
            ConfigAllInfo configInfo = configInfoList.get(i);
            try {
                ParamUtils
                        .checkParam(configInfo.getDataId(), configInfo.getGroup(), "datumId", configInfo.getContent());
            } catch (NacosException e) {
                LogUtil.DEFAULT_LOG.error("data verification failed", e);
                throw e;
            }
            ConfigInfo configInfo2Save = new ConfigInfo(configInfo.getDataId(), configInfo.getGroup(),
                    configInfo.getTenant(), configInfo.getAppName(), configInfo.getContent());
            
            String type = configInfo.getType();
            if (StringUtils.isBlank(type)) {
                // simple judgment of file type based on suffix
                if (configInfo.getDataId().contains(SPOT)) {
                    String extName = configInfo.getDataId().substring(configInfo.getDataId().lastIndexOf(SPOT) + 1);
                    FileTypeEnum fileTypeEnum = FileTypeEnum.getFileTypeEnumByFileExtensionOrFileType(extName);
                    type = fileTypeEnum.getFileType();
                }
            }
            if (configAdvanceInfo == null) {
                configAdvanceInfo = new HashMap<>(16);
            }
            configAdvanceInfo.put("type", type);
            configAdvanceInfo.put("desc", configInfo.getDesc());
            try {
                addConfigInfo(srcIp, srcUser, configInfo2Save, time, configAdvanceInfo, notify);
                succCount++;
            } catch (DataIntegrityViolationException ive) {
                // uniqueness constraint conflict
                if (SameConfigPolicy.ABORT.equals(policy)) {
                    failData = new ArrayList<>();
                    skipData = new ArrayList<>();
                    Map<String, String> faileditem = new HashMap<>(2);
                    faileditem.put("dataId", configInfo2Save.getDataId());
                    faileditem.put("group", configInfo2Save.getGroup());
                    failData.add(faileditem);
                    for (int j = (i + 1); j < configInfoList.size(); j++) {
                        ConfigInfo skipConfigInfo = configInfoList.get(j);
                        Map<String, String> skipitem = new HashMap<>(2);
                        skipitem.put("dataId", skipConfigInfo.getDataId());
                        skipitem.put("group", skipConfigInfo.getGroup());
                        skipData.add(skipitem);
                    }
                    break;
                } else if (SameConfigPolicy.SKIP.equals(policy)) {
                    skipCount++;
                    if (skipData == null) {
                        skipData = new ArrayList<>();
                    }
                    Map<String, String> skipitem = new HashMap<>(2);
                    skipitem.put("dataId", configInfo2Save.getDataId());
                    skipitem.put("group", configInfo2Save.getGroup());
                    skipData.add(skipitem);
                } else if (SameConfigPolicy.OVERWRITE.equals(policy)) {
                    succCount++;
                    updateConfigInfo(configInfo2Save, srcIp, srcUser, time, configAdvanceInfo, notify);
                }
            }
        }
        Map<String, Object> result = new HashMap<>(4);
        result.put("succCount", succCount);
        result.put("skipCount", skipCount);
        if (failData != null && !failData.isEmpty()) {
            result.put("failData", failData);
        }
        if (skipData != null && !skipData.isEmpty()) {
            result.put("skipData", skipData);
        }
        return result;
    }
    
    @Override
    public int tenantInfoCountByTenantId(String tenantId) {
        return tenantInfoRepository.tenantInfoCountByTenantId(tenantId);
    }
    
}
