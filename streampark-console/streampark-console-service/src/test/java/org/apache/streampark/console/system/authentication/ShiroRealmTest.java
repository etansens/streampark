/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.streampark.console.system.authentication;

import org.apache.streampark.console.core.enums.AuthenticationType;
import org.apache.streampark.console.system.entity.AccessToken;
import org.apache.streampark.console.system.entity.User;
import org.apache.streampark.console.system.service.AccessTokenService;
import org.apache.streampark.console.system.service.UserService;

import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.mgt.DefaultSecurityManager;
import org.apache.shiro.subject.Subject;
import org.apache.shiro.util.ThreadContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ShiroRealmTest {

  @Mock private UserService userService;

  @Mock private AccessTokenService accessTokenService;

  @InjectMocks private TestableShiroRealm shiroRealm;

  @AfterEach
  void tearDown() {
    ThreadContext.unbindSubject();
    ThreadContext.unbindSecurityManager();
  }

  @Test
  void authenticatesStoredOpenAPIToken() throws Exception {
    bindSubject();
    User user = new User();
    user.setUserId(100000L);
    user.setUsername("admin");
    user.setStatus(User.STATUS_VALID);

    String encryptedToken = JWTUtil.sign(user, AuthenticationType.OPENAPI, Long.MAX_VALUE);
    String credential = JWTUtil.decrypt(encryptedToken);
    AccessToken accessToken = new AccessToken();
    accessToken.setToken(encryptedToken);
    accessToken.setStatus(AccessToken.STATUS_ENABLE);

    when(userService.findByName("admin")).thenReturn(user);
    when(accessTokenService.getByUserId(100000L)).thenReturn(accessToken);

    AuthenticationInfo authenticationInfo =
        Assertions.assertDoesNotThrow(() -> shiroRealm.authenticate(new JWTToken(credential)));

    Assertions.assertEquals(credential, authenticationInfo.getPrincipals().getPrimaryPrincipal());
  }

  private void bindSubject() {
    DefaultSecurityManager securityManager = new DefaultSecurityManager();
    ThreadContext.bind(securityManager);
    Subject subject = new Subject.Builder(securityManager).buildSubject();
    ThreadContext.bind(subject);
  }

  private static class TestableShiroRealm extends ShiroRealm {
    AuthenticationInfo authenticate(AuthenticationToken token) {
      return doGetAuthenticationInfo(token);
    }
  }
}
