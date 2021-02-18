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

import com.alibaba.nacos.config.server.auth.RoleInfo;
import com.alibaba.nacos.config.server.model.Page;

import java.util.List;

public interface RolesRepository {
    
    /**
     * Execute getRoles operation
     *
     * @param pageNo   pageNo
     * @param pageSize pageSize
     * @return role list
     */
    Page<RoleInfo> getRoles(int pageNo, int pageSize);
    
    /**
     * Execute getRoles by username operation
     *
     * @param username username
     * @param pageNo   pageNo
     * @param pageSize pageSize
     * @return role list
     */
    Page<RoleInfo> getRolesByUserName(String username, int pageNo, int pageSize);
    
    /**
     * Execute add role operation.
     *
     * @param role     role string value.
     * @param userName username string value.
     */
    void addRole(String role, String userName);
    
    /**
     * Execute delete role operation.
     *
     * @param role role string value.
     */
    void deleteRole(String role);
    
    /**
     * Execute delete role operation.
     *
     * @param role     role string value.
     * @param username username string value.
     */
    void deleteRole(String role, String username);
    
    /**
     * Execute find roles like role name operation
     *
     * @param role role name
     * @return role list
     */
    List<String> findRolesLikeRoleName(String role);
}
