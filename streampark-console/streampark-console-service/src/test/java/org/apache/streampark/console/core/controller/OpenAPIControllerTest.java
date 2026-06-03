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

package org.apache.streampark.console.core.controller;

import org.apache.streampark.console.base.domain.RestResponse;
import org.apache.streampark.console.core.annotation.OpenAPI;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.Savepoint;
import org.apache.streampark.console.core.enums.FlinkAppState;
import org.apache.streampark.console.core.service.AppBuildPipeService;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.SavepointService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.servlet.http.HttpServletRequest;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OpenAPIControllerTest {

  @Mock private ApplicationService applicationService;

  @Mock private AppBuildPipeService appBuildPipeService;

  @Mock private SavepointService savepointService;

  @Mock private HttpServletRequest httpServletRequest;

  @InjectMocks private OpenAPIController openAPIController;

  @Test
  void flinkGetReturnsApplicationFromService() {
    Application request = new Application();
    request.setId(100L);
    Application application = new Application();
    application.setId(100L);
    application.setJobName("openapi-get-test");
    when(applicationService.getApp(request)).thenReturn(application);

    RestResponse response = openAPIController.flinkGet(request);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Assertions.assertSame(application, response.get("data"));
    verify(applicationService).getApp(request);
  }

  @Test
  void flinkCopyReturnsNewApplicationIdWhenCopySucceeds() throws Exception {
    Application request = new Application();
    request.setId(100L);
    request.setJobName("copied-job");
    when(applicationService.copy(request)).thenReturn(200L);

    RestResponse response = openAPIController.flinkCopy(request);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Assertions.assertEquals("200", ((Map<?, ?>) response.get("data")).get("id"));
    verify(applicationService).copy(request);
  }

  @Test
  void flinkCopyReturnsFalseWhenServiceReturnsZeroId() throws Exception {
    Application request = new Application();
    request.setId(100L);
    when(applicationService.copy(request)).thenReturn(0L);

    RestResponse response = openAPIController.flinkCopy(request);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Assertions.assertEquals("0", ((Map<?, ?>) response.get("data")).get("id"));
    verify(applicationService).copy(request);
  }

  @Test
  void flinkCreateReturnsCreatedApplicationId() throws Exception {
    Application request = new Application();
    request.setId(300L);
    request.setJobName("created-job");
    when(applicationService.create(request)).thenReturn(true);

    RestResponse response = openAPIController.flinkCreate(request);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Map<?, ?> data = (Map<?, ?>) response.get("data");
    Assertions.assertEquals(Boolean.TRUE, data.get("success"));
    Assertions.assertEquals(300L, data.get("id"));
    verify(applicationService).create(request);
  }

  @Test
  void flinkUpdateMergesExplicitArgsAndPreservesMissingFields() {
    Application request = new Application();
    request.setId(400L);
    request.setArgs("--new-args");

    Application existing = new Application();
    existing.setId(400L);
    existing.setTeamId(100000L);
    existing.setJobName("existing-job");
    existing.setExecutionMode(6);
    existing.setVersionId(10000L);
    existing.setArgs("--old-args");
    existing.setDynamicProperties("-Dparallelism.default=2");
    existing.setOptions("{}");
    existing.setK8sPodTemplate("pod-template-yaml");
    existing.setFlinkSql(
        Base64.getEncoder().encodeToString("SELECT 1".getBytes(StandardCharsets.UTF_8)));
    when(applicationService.getApp(org.mockito.ArgumentMatchers.any(Application.class)))
        .thenReturn(existing);
    when(httpServletRequest.getParameterMap())
        .thenReturn(parameterMap("id", "400", "args", "--new-args"));

    RestResponse response = openAPIController.flinkUpdate(request, httpServletRequest);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Assertions.assertEquals(Boolean.TRUE, response.get("data"));

    ArgumentCaptor<Application> updateCaptor = ArgumentCaptor.forClass(Application.class);
    verify(applicationService).update(updateCaptor.capture());
    Application merged = updateCaptor.getValue();
    Assertions.assertEquals(400L, merged.getId());
    Assertions.assertEquals(100000L, merged.getTeamId());
    Assertions.assertEquals("existing-job", merged.getJobName());
    Assertions.assertEquals(6, merged.getExecutionMode());
    Assertions.assertEquals(10000L, merged.getVersionId());
    Assertions.assertEquals("--new-args", merged.getArgs());
    Assertions.assertEquals("-Dparallelism.default=2", merged.getDynamicProperties());
    Assertions.assertEquals("{}", merged.getOptions());
    Assertions.assertEquals("pod-template-yaml", merged.getK8sPodTemplate());
    Assertions.assertEquals("SELECT 1", merged.getFlinkSql());
  }

  @Test
  void flinkUpdateClearsExplicitEmptyFields() {
    Application request = new Application();
    request.setId(401L);
    request.setDynamicProperties("");
    request.setOptions("");

    Application existing = new Application();
    existing.setId(401L);
    existing.setDynamicProperties("-Dparallelism.default=2");
    existing.setOptions("{}");
    when(applicationService.getApp(org.mockito.ArgumentMatchers.any(Application.class)))
        .thenReturn(existing);
    when(httpServletRequest.getParameterMap())
        .thenReturn(parameterMap("id", "401", "dynamicProperties", "", "options", ""));

    RestResponse response = openAPIController.flinkUpdate(request, httpServletRequest);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Assertions.assertEquals(Boolean.TRUE, response.get("data"));

    ArgumentCaptor<Application> updateCaptor = ArgumentCaptor.forClass(Application.class);
    verify(applicationService).update(updateCaptor.capture());
    Application merged = updateCaptor.getValue();
    Assertions.assertEquals("", merged.getDynamicProperties());
    Assertions.assertEquals("", merged.getOptions());
  }

  @Test
  void flinkUpdateIgnoresExplicitTeamId() {
    Application request = new Application();
    request.setId(402L);
    request.setTeamId(999999L);
    request.setArgs("--new-args");

    Application existing = new Application();
    existing.setId(402L);
    existing.setTeamId(100000L);
    existing.setArgs("--old-args");
    when(applicationService.getApp(org.mockito.ArgumentMatchers.any(Application.class)))
        .thenReturn(existing);
    when(httpServletRequest.getParameterMap())
        .thenReturn(parameterMap("id", "402", "teamId", "999999", "args", "--new-args"));

    RestResponse response = openAPIController.flinkUpdate(request, httpServletRequest);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));

    ArgumentCaptor<Application> updateCaptor = ArgumentCaptor.forClass(Application.class);
    verify(applicationService).update(updateCaptor.capture());
    Application merged = updateCaptor.getValue();
    Assertions.assertEquals(100000L, merged.getTeamId());
    Assertions.assertEquals("--new-args", merged.getArgs());
  }

  @Test
  void flinkUpdateSchemaIncludesArgsAndKubernetesPodTemplatesWithoutJar() throws Exception {
    OpenAPI openAPI =
        OpenAPIController.class
            .getDeclaredMethod("flinkUpdate", Application.class, HttpServletRequest.class)
            .getDeclaredAnnotation(OpenAPI.class);

    Set<String> paramNames =
        Stream.of(openAPI.param()).map(OpenAPI.Param::name).collect(Collectors.toSet());

    Assertions.assertTrue(paramNames.contains("args"));
    Assertions.assertTrue(paramNames.contains("k8sPodTemplate"));
    Assertions.assertTrue(paramNames.contains("k8sJmPodTemplate"));
    Assertions.assertTrue(paramNames.contains("k8sTmPodTemplate"));
    Assertions.assertFalse(paramNames.contains("jar"));
  }

  private Map<String, String[]> parameterMap(String... values) {
    Map<String, String[]> parameters = new HashMap<>();
    for (int i = 0; i < values.length; i += 2) {
      parameters.put(values[i], new String[] {values[i + 1]});
    }
    return parameters;
  }

  @Test
  void flinkBuildReturnsBuildApplicationResponse() throws Exception {
    RestResponse buildResponse = RestResponse.success(true);
    when(applicationService.buildApplication(500L, true)).thenReturn(buildResponse);

    RestResponse response = openAPIController.flinkBuild(500L, true);

    Assertions.assertSame(buildResponse, response);
    verify(applicationService).buildApplication(500L, true);
  }

  @Test
  void flinkBuildStatusReturnsNullWhenNoPipelineExists() {
    when(appBuildPipeService.getCurrentBuildPipeline(600L)).thenReturn(Optional.empty());

    RestResponse response = openAPIController.flinkBuildStatus(600L);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Assertions.assertNull(response.get("data"));
    verify(appBuildPipeService).getCurrentBuildPipeline(600L);
  }

  @Test
  void flinkRestartCancelsWaitsUntilStartableThenStarts() throws Exception {
    Application request = new Application();
    request.setId(700L);
    request.setRestoreOrTriggerSavepoint(false);
    request.setAllowNonRestored(true);
    request.setSavepointPath("file:///savepoints/sp-1");
    request.setSavepointTimeout(1L);

    Application stoppedApplication = new Application();
    stoppedApplication.setId(700L);
    stoppedApplication.setState(FlinkAppState.CANCELED.getValue());
    when(applicationService.getById(700L)).thenReturn(stoppedApplication);

    OpenAPIController.RestartOptions options = new OpenAPIController.RestartOptions();
    options.setRestoreFromSavepoint(true);

    RestResponse response = openAPIController.flinkRestart(request, options);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Assertions.assertEquals(Boolean.TRUE, response.get("data"));

    ArgumentCaptor<Application> startCaptor = ArgumentCaptor.forClass(Application.class);
    InOrder order = inOrder(applicationService);
    order.verify(applicationService).cancel(request);
    order.verify(applicationService).getById(700L);
    order.verify(applicationService).start(startCaptor.capture(), eq(false));

    Application startParam = startCaptor.getValue();
    Assertions.assertEquals(700L, startParam.getId());
    Assertions.assertTrue(startParam.getRestoreOrTriggerSavepoint());
    Assertions.assertTrue(startParam.getAllowNonRestored());
    Assertions.assertEquals("file:///savepoints/sp-1", startParam.getSavepointPath());
  }

  @Test
  void flinkSavepointTriggerDelegatesToSavepointService() throws Exception {
    RestResponse response =
        openAPIController.flinkSavepointTrigger(800L, "file:///savepoints/sp-2");

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Assertions.assertEquals(Boolean.TRUE, response.get("data"));
    verify(savepointService).trigger(800L, "file:///savepoints/sp-2");
  }

  @Test
  void flinkSavepointLatestReturnsLatestSavepoint() {
    Savepoint savepoint = new Savepoint();
    savepoint.setAppId(900L);
    savepoint.setPath("file:///savepoints/latest");
    when(savepointService.getLatest(900L)).thenReturn(savepoint);

    RestResponse response = openAPIController.flinkSavepointLatest(900L);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Assertions.assertSame(savepoint, response.get("data"));
    verify(savepointService).getLatest(900L);
  }
}
