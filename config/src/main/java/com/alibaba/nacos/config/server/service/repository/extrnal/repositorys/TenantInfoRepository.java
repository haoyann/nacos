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

import com.alibaba.nacos.config.server.model.TenantInfo;

import java.util.List;

public interface TenantInfoRepository {
    
    /**
     * insert tenant info.
     *
     * @param kp            kp
     * @param tenantId      tenant Id
     * @param tenantName    tenant name
     * @param tenantDesc    tenant description
     * @param createResoure create resouce
     * @param time          time
     */
    void insertTenantInfoAtomic(String kp, String tenantId, String tenantName, String tenantDesc, String createResoure,
            final long time);
    
    /**
     * Update tenantInfo showname.
     *
     * @param kp         kp
     * @param tenantId   tenant Id
     * @param tenantName tenant name
     * @param tenantDesc tenant description
     */
    void updateTenantNameAtomic(String kp, String tenantId, String tenantName, String tenantDesc);
    
    /**
     * Query tenant info.
     *
     * @param kp kp
     * @return {@link TenantInfo} list
     */
    List<TenantInfo> findTenantByKp(String kp);
    
    /**
     * Query tenant info.
     *
     * @param kp       kp
     * @param tenantId tenant id
     * @return {@link TenantInfo}
     */
    TenantInfo findTenantByKp(String kp, String tenantId);
    
    /**
     * Remote tenant info.
     *
     * @param kp       kp
     * @param tenantId tenant id
     */
    void removeTenantInfoAtomic(final String kp, final String tenantId);
    
    /**
     * query tenantInfo (namespace) existence based by tenantId.
     *
     * @param tenantId tenant Id
     * @return count by tenantId
     */
    int tenantInfoCountByTenantId(String tenantId);
}
