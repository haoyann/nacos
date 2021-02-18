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

package com.alibaba.nacos.config.server.auth;

import com.alibaba.nacos.config.server.configuration.ConditionOnExternalStorage;
import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.RolesRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Implemetation of ExternalRolePersistServiceImpl.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
@Conditional(value = ConditionOnExternalStorage.class)
@Component
public class ExternalRolePersistServiceImpl implements RolePersistService {
    
    @Autowired
    private RolesRepository rolesRepository;
    
    public Page<RoleInfo> getRoles(int pageNo, int pageSize) {
        return rolesRepository.getRoles(pageNo, pageSize);
    }
    
    public Page<RoleInfo> getRolesByUserName(String username, int pageNo, int pageSize) {
        return rolesRepository.getRolesByUserName(username, pageNo, pageSize);
    }
    
    /**
     * Execute add role operation.
     *
     * @param role     role string value.
     * @param userName username string value.
     */
    public void addRole(String role, String userName) {
        rolesRepository.addRole(role, userName);
    }
    
    /**
     * Execute delete role operation.
     *
     * @param role role string value.
     */
    public void deleteRole(String role) {
        rolesRepository.deleteRole(role);
    }
    
    /**
     * Execute delete role operation.
     *
     * @param role     role string value.
     * @param username username string value.
     */
    public void deleteRole(String role, String username) {
        rolesRepository.deleteRole(role, username);
    }
    
    @Override
    public List<String> findRolesLikeRoleName(String role) {
        return rolesRepository.findRolesLikeRoleName(role);
    }
    
    private static final class RoleInfoRowMapper implements RowMapper<RoleInfo> {
        
        @Override
        public RoleInfo mapRow(ResultSet rs, int rowNum) throws SQLException {
            RoleInfo roleInfo = new RoleInfo();
            roleInfo.setRole(rs.getString("role"));
            roleInfo.setUsername(rs.getString("username"));
            return roleInfo;
        }
    }
}
