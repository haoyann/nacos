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

package com.alibaba.nacos.config.server.service.datasource.multiple;

import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.ConfigInfoAggrRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.ConfigInfoBetaRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.ConfigInfoRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.ConfigInfoTagRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.ConfigTagsRelationRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.HisConfigInfoRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.PermissionsRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.RolesRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.TenantInfoRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.UsersRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.jdbc.JDBCConfigInfoAggrRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.jdbc.JDBCConfigInfoBetaRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.jdbc.JDBCConfigInfoRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.jdbc.JDBCConfigInfoTagRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.jdbc.JDBCConfigTagsRelationRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.jdbc.JDBCHisConfigInfoRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.jdbc.JDBCPermissionsRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.jdbc.JDBCRolesRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.jdbc.JDBCTenantInfoRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.jdbc.JDBCUsersRepository;
import com.alibaba.nacos.config.server.service.repository.extrnal.dialect.DialectExistTable;
import com.alibaba.nacos.config.server.service.repository.extrnal.dialect.DialectPageHelper;
import com.alibaba.nacos.config.server.service.repository.extrnal.dialect.mysql.MYSQLExistTable;
import com.alibaba.nacos.config.server.service.repository.extrnal.dialect.mysql.MYSQLPageHelper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@ConditionalOnExpression("'${spring.datasource.platform}'.equalsIgnoreCase('mysql')")
@Configuration
public class MYSQLDataSourceConfig {
    
    @Bean
    public ConfigInfoAggrRepository registerConfigInfoAggrRepository() {
        return new JDBCConfigInfoAggrRepository();
    }
    
    @Bean
    public ConfigInfoBetaRepository registerConfigInfoBetaRepository() {
        return new JDBCConfigInfoBetaRepository();
    }
    
    @Bean
    public ConfigInfoRepository registerConfigInfoRepository() {
        return new JDBCConfigInfoRepository();
    }
    
    @Bean
    public ConfigInfoTagRepository registerConfigInfoTagRepository() {
        return new JDBCConfigInfoTagRepository();
    }
    
    @Bean
    public ConfigTagsRelationRepository registerConfigTagsRelationRepository() {
        return new JDBCConfigTagsRelationRepository();
    }
    
    @Bean
    public HisConfigInfoRepository registerHisConfigInfoRepository() {
        return new JDBCHisConfigInfoRepository();
    }
    
    @Bean
    public PermissionsRepository registerPermissionsRepository() {
        return new JDBCPermissionsRepository();
    }
    
    @Bean
    public RolesRepository registerRolesRepository() {
        return new JDBCRolesRepository();
    }
    
    @Bean
    public TenantInfoRepository registerTenantInfoRepository() {
        return new JDBCTenantInfoRepository();
    }
    
    @Bean
    public UsersRepository registerUsersRepository() {
        return new JDBCUsersRepository();
    }
    
    @Bean
    public DialectExistTable registerDialectExistTable() {
        return new MYSQLExistTable();
    }
    
    @Bean
    public DialectPageHelper registerDialectPageHelper() {
        return new MYSQLPageHelper();
    }
}
