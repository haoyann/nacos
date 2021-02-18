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
import com.alibaba.nacos.config.server.model.User;
import com.alibaba.nacos.config.server.service.repository.extrnal.repositorys.UsersRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Conditional;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Implemetation of ExternalUserPersistServiceImpl.
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
@Conditional(value = ConditionOnExternalStorage.class)
@Component
public class ExternalUserPersistServiceImpl implements UserPersistService {
    
    @Autowired
    private UsersRepository usersRepository;
    
    /**
     * Execute create user operation.
     *
     * @param username username string value.
     * @param password password string value.
     */
    public void createUser(String username, String password) {
        usersRepository.createUser(username, password);
    }
    
    /**
     * Execute delete user operation.
     *
     * @param username username string value.
     */
    public void deleteUser(String username) {
        usersRepository.deleteUser(username);
    }
    
    /**
     * Execute update user password operation.
     *
     * @param username username string value.
     * @param password password string value.
     */
    public void updateUserPassword(String username, String password) {
        usersRepository.updateUserPassword(username, password);
    }
    
    /**
     * Execute find user by username operation.
     *
     * @param username username string value.
     * @return User model.
     */
    public User findUserByUsername(String username) {
        return usersRepository.findUserByUsername(username);
    }
    
    public Page<User> getUsers(int pageNo, int pageSize) {
        return usersRepository.getUsers(pageNo, pageSize);
    }
    
    @Override
    public List<String> findUserLikeUsername(String username) {
        return usersRepository.findUserLikeUsername(username);
    }
}
