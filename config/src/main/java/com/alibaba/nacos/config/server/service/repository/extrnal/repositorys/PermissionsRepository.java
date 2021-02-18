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

import com.alibaba.nacos.config.server.auth.PermissionInfo;
import com.alibaba.nacos.config.server.model.Page;

public interface PermissionsRepository {
    
    /**
     * Execute get permissions operation.
     *
     * @param role     role
     * @param pageNo   pageNo
     * @param pageSize pageSize
     * @return permission list
     */
    Page<PermissionInfo> getPermissions(String role, int pageNo, int pageSize);
    
    /**
     * Execute add permission operation.
     *
     * @param role     role string value.
     * @param resource resource string value.
     * @param action   action string value.
     */
    void addPermission(String role, String resource, String action);
    
    /**
     * Execute delete permission operation.
     *
     * @param role     role string value.
     * @param resource resource string value.
     * @param action   action string value.
     */
    void deletePermission(String role, String resource, String action);
}
