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

package org.apache.streampark.console.core.component;

import org.apache.streampark.console.core.service.ServiceHelper;
import org.apache.streampark.console.system.entity.AccessToken;
import org.apache.streampark.console.system.service.AccessTokenService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OpenAPIComponentTest {

  @Mock private AccessTokenService accessTokenService;

  @Mock private ServiceHelper serviceHelper;

  @InjectMocks private OpenAPIComponent openAPIComponent;

  @Test
  void getOpenApiCUrlFillsJobNameBasedRequiredParameters() {
    AccessToken accessToken = new AccessToken();
    accessToken.setToken("openapi-token");
    when(serviceHelper.getUserId()).thenReturn(1L);
    when(accessTokenService.getByUserId(1L)).thenReturn(accessToken);

    String curl =
        openAPIComponent.getOpenApiCUrl(
            "flinkCopy", "http://localhost:10000", 100L, 100000L, "source-job");

    Assertions.assertTrue(curl.contains("--data-urlencode 'srcJobName=source-job'"));
    Assertions.assertTrue(curl.contains("--data-urlencode 'dstJobName=source-job-copy'"));
    Assertions.assertFalse(curl.contains("--data-urlencode 'teamId="));
    Assertions.assertFalse(curl.contains("--data-urlencode 'argument="));
  }
}
