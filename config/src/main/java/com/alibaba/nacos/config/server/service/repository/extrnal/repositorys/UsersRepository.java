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

import com.alibaba.nacos.config.server.model.Page;
import com.alibaba.nacos.config.server.model.User;

import java.util.List;

public interface UsersRepository {
    
    /**
     * Execute create user operation.
     *
     * @param username username string value.
     * @param password password string value.
     */
    void createUser(String username, String password);
    
    /**
     * Execute delete user operation.
     *
     * @param username username string value.
     */
    void deleteUser(String username);
    
    /**
     * Execute update user password operation.
     *
     * @param username username string value.
     * @param password password string value.
     */
    void updateUserPassword(String username, String password);
    
    /**
     * Execute find user by username operation.
     *
     * @param username username string value.
     * @return User model.
     */
    User findUserByUsername(String username);
    
    /**
     * Execute getUsers operation.
     *
     * @param pageNo   pageNo
     * @param pageSize pageSize
     * @return user list
     */
    Page<User> getUsers(int pageNo, int pageSize);
    
    /**
     * Execute find user like username operation.
     *
     * @param username like username
     * @return username list
     */
    List<String> findUserLikeUsername(String username);
}
