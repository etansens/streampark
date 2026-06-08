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
import org.apache.streampark.console.base.exception.ApiAlertException;
import org.apache.streampark.console.core.annotation.OpenAPI;
import org.apache.streampark.console.core.component.OpenAPIComponent;
import org.apache.streampark.console.core.entity.AppBuildPipeline;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.Savepoint;
import org.apache.streampark.console.core.enums.FlinkAppState;
import org.apache.streampark.console.core.service.AppBuildPipeService;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.SavepointService;
import org.apache.streampark.flink.packer.pipeline.PipelineStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.bind.annotation.PostMapping;

import javax.servlet.http.HttpServletRequest;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OpenAPIControllerTest {

  @Mock private ApplicationService applicationService;

  @Mock private AppBuildPipeService appBuildPipeService;

  @Mock private SavepointService savepointService;

  @Mock private OpenAPIComponent openAPIComponent;

  @Mock private HttpServletRequest httpServletRequest;

  @InjectMocks private OpenAPIController openAPIController;

  @Test
  void flinkGetResolvesApplicationByJobName() {
    Application request = new Application();
    request.setJobName("openapi-get-test");
    Application resolved = new Application();
    resolved.setId(100L);
    resolved.setJobName("openapi-get-test");
    Application application = new Application();
    application.setId(100L);
    application.setJobName("openapi-get-test");
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(resolved));
    when(applicationService.getApp(org.mockito.ArgumentMatchers.any(Application.class)))
        .thenReturn(application);

    RestResponse response = openAPIController.flinkGet(request);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Assertions.assertSame(application, response.get("data"));
    ArgumentCaptor<Application> queryCaptor = ArgumentCaptor.forClass(Application.class);
    verify(applicationService).getApp(queryCaptor.capture());
    Assertions.assertEquals(100L, queryCaptor.getValue().getId());
  }

  @Test
  void flinkCopyReturnsNewApplicationIdWhenCopySucceeds() throws Exception {
    OpenAPIController.CopyRequest request = new OpenAPIController.CopyRequest();
    request.setSrcJobName("source-job");
    request.setDstJobName("copied-job");

    Application source = new Application();
    source.setId(100L);
    source.setJobName("source-job");
    source.setTeamId(100000L);
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(source));
    when(applicationService.copy(org.mockito.ArgumentMatchers.any(Application.class)))
        .thenReturn(200L);

    RestResponse response = openAPIController.flinkCopy(request);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Assertions.assertEquals("200", ((Map<?, ?>) response.get("data")).get("id"));
    ArgumentCaptor<Application> copyCaptor = ArgumentCaptor.forClass(Application.class);
    verify(applicationService).copy(copyCaptor.capture());
    Application copied = copyCaptor.getValue();
    Assertions.assertEquals(100L, copied.getId());
    Assertions.assertEquals("copied-job", copied.getJobName());
    Assertions.assertNull(copied.getArgs());
    Assertions.assertEquals(100000L, copied.getTeamId());
  }

  @Test
  void flinkCopyReturnsFalseWhenServiceReturnsZeroId() throws Exception {
    OpenAPIController.CopyRequest request = new OpenAPIController.CopyRequest();
    request.setSrcJobName("source-job");
    request.setDstJobName("copied-job");
    Application source = new Application();
    source.setId(100L);
    source.setJobName("source-job");
    source.setTeamId(100000L);
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(source));
    when(applicationService.copy(org.mockito.ArgumentMatchers.any(Application.class)))
        .thenReturn(0L);

    RestResponse response = openAPIController.flinkCopy(request);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Assertions.assertEquals("0", ((Map<?, ?>) response.get("data")).get("id"));
    verify(applicationService).copy(org.mockito.ArgumentMatchers.any(Application.class));
  }

  @Test
  void flinkCopyRejectsMissingSourceApplication() throws Exception {
    OpenAPIController.CopyRequest request = new OpenAPIController.CopyRequest();
    request.setSrcJobName("missing-source");
    request.setDstJobName("missing-source-copy");
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.emptyList());

    ApiAlertException exception =
        Assertions.assertThrows(
            ApiAlertException.class, () -> openAPIController.flinkCopy(request));

    Assertions.assertEquals(
        "The application jobName=missing-source not found, copy failed.", exception.getMessage());
    verify(applicationService, never()).copy(org.mockito.ArgumentMatchers.any(Application.class));
  }

  @Test
  void flinkCopySchemaOnlyAcceptsSourceAndDestinationJobNames() throws Exception {
    OpenAPI openAPI =
        OpenAPIController.class
            .getDeclaredMethod("flinkCopy", OpenAPIController.CopyRequest.class)
            .getDeclaredAnnotation(OpenAPI.class);

    Set<String> paramNames =
        Stream.of(openAPI.param()).map(OpenAPI.Param::name).collect(Collectors.toSet());

    Assertions.assertTrue(paramNames.contains("srcJobName"));
    Assertions.assertTrue(paramNames.contains("dstJobName"));
    Assertions.assertEquals(2, paramNames.size());
    Assertions.assertFalse(paramNames.contains("id"));
    Assertions.assertFalse(paramNames.contains("jobName"));
    Assertions.assertFalse(paramNames.contains("teamId"));
    Assertions.assertFalse(paramNames.contains("argument"));
  }

  @Test
  void flinkCreateIsNotExposedAsOpenApiEndpoint() {
    Set<String> openApiNames =
        Stream.of(OpenAPIController.class.getDeclaredMethods())
            .map(method -> method.getDeclaredAnnotation(OpenAPI.class))
            .filter(annotation -> annotation != null)
            .map(OpenAPI::name)
            .collect(Collectors.toSet());

    boolean hasCreateMapping =
        Stream.of(OpenAPIController.class.getDeclaredMethods())
            .map(method -> method.getDeclaredAnnotation(PostMapping.class))
            .filter(annotation -> annotation != null)
            .anyMatch(annotation -> Arrays.asList(annotation.value()).contains("app/create"));

    Assertions.assertFalse(openApiNames.contains("flinkCreate"));
    Assertions.assertFalse(hasCreateMapping);
  }

  @Test
  void flinkUpdateMergesExplicitArgsAndPreservesMissingFields() {
    Application request = new Application();
    request.setJobName("existing-job");
    request.setArgs("--new-args");

    Application resolved = new Application();
    resolved.setId(400L);
    resolved.setJobName("existing-job");
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
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(resolved));
    when(applicationService.getApp(org.mockito.ArgumentMatchers.any(Application.class)))
        .thenReturn(existing);
    when(httpServletRequest.getParameterMap())
        .thenReturn(parameterMap("jobName", "existing-job", "args", "--new-args"));

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
    request.setJobName("empty-field-job");
    request.setDynamicProperties("");
    request.setOptions("");

    Application resolved = new Application();
    resolved.setId(401L);
    resolved.setJobName("empty-field-job");
    Application existing = new Application();
    existing.setId(401L);
    existing.setDynamicProperties("-Dparallelism.default=2");
    existing.setOptions("{}");
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(resolved));
    when(applicationService.getApp(org.mockito.ArgumentMatchers.any(Application.class)))
        .thenReturn(existing);
    when(httpServletRequest.getParameterMap())
        .thenReturn(
            parameterMap("jobName", "empty-field-job", "dynamicProperties", "", "options", ""));

    RestResponse response = openAPIController.flinkUpdate(request, httpServletRequest);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Assertions.assertEquals(Boolean.TRUE, response.get("data"));

    ArgumentCaptor<Application> updateCaptor = ArgumentCaptor.forClass(Application.class);
    verify(applicationService).update(updateCaptor.capture());
    Application merged = updateCaptor.getValue();
    Assertions.assertEquals("", merged.getDynamicProperties());
    Assertions.assertEquals("{}", merged.getOptions());
  }

  @Test
  void flinkUpdateIgnoresExplicitTeamId() {
    Application request = new Application();
    request.setJobName("team-safe-job");
    request.setTeamId(999999L);
    request.setArgs("--new-args");

    Application resolved = new Application();
    resolved.setId(402L);
    resolved.setJobName("team-safe-job");
    Application existing = new Application();
    existing.setId(402L);
    existing.setTeamId(100000L);
    existing.setArgs("--old-args");
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(resolved));
    when(applicationService.getApp(org.mockito.ArgumentMatchers.any(Application.class)))
        .thenReturn(existing);
    when(httpServletRequest.getParameterMap())
        .thenReturn(
            parameterMap("jobName", "team-safe-job", "teamId", "999999", "args", "--new-args"));

    RestResponse response = openAPIController.flinkUpdate(request, httpServletRequest);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));

    ArgumentCaptor<Application> updateCaptor = ArgumentCaptor.forClass(Application.class);
    verify(applicationService).update(updateCaptor.capture());
    Application merged = updateCaptor.getValue();
    Assertions.assertEquals(100000L, merged.getTeamId());
    Assertions.assertEquals("--new-args", merged.getArgs());
  }

  @Test
  void flinkUpdateIgnoresUnsupportedOpenApiOverlayFields() {
    Application request = new Application();
    request.setJobName("openapi-update-contract");
    request.setMainClass("org.example.NewJob");
    request.setFlinkSql("SELECT 2");
    request.setArgs("--new-args");
    request.setDynamicProperties("-Dnew=true");
    request.setFlinkImage("registry/flink:new");
    request.setK8sPodTemplate("pod-template-new");
    request.setJar("/tmp/new.jar");
    request.setOptions("{\"new\":true}");
    request.setYarnQueue("new-queue");

    Application resolved = new Application();
    resolved.setId(440L);
    resolved.setJobName("openapi-update-contract");
    Application existing = new Application();
    existing.setId(440L);
    existing.setJobName("openapi-update-contract");
    existing.setMainClass("org.example.OldJob");
    existing.setFlinkSql(
        Base64.getEncoder().encodeToString("SELECT 1".getBytes(StandardCharsets.UTF_8)));
    existing.setArgs("--old-args");
    existing.setDynamicProperties("-Dold=true");
    existing.setFlinkImage("registry/flink:old");
    existing.setK8sPodTemplate("pod-template-old");
    existing.setJar("/tmp/old.jar");
    existing.setOptions("{\"old\":true}");
    existing.setYarnQueue("old-queue");
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(resolved));
    when(applicationService.getApp(org.mockito.ArgumentMatchers.any(Application.class)))
        .thenReturn(existing);
    when(httpServletRequest.getParameterMap())
        .thenReturn(
            parameterMap(
                "jobName", "openapi-update-contract",
                "mainClass", "org.example.NewJob",
                "flinkSql", "SELECT 2",
                "args", "--new-args",
                "dynamicProperties", "-Dnew=true",
                "flinkImage", "registry/flink:new",
                "k8sPodTemplate", "pod-template-new",
                "jar", "/tmp/new.jar",
                "options", "{\"new\":true}",
                "yarnQueue", "new-queue"));

    RestResponse response = openAPIController.flinkUpdate(request, httpServletRequest);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    ArgumentCaptor<Application> captor = ArgumentCaptor.forClass(Application.class);
    verify(applicationService).update(captor.capture());
    Application updated = captor.getValue();
    Assertions.assertEquals("org.example.NewJob", updated.getMainClass());
    Assertions.assertEquals("SELECT 2", updated.getFlinkSql());
    Assertions.assertEquals("--new-args", updated.getArgs());
    Assertions.assertEquals("-Dnew=true", updated.getDynamicProperties());
    Assertions.assertEquals("registry/flink:new", updated.getFlinkImage());
    Assertions.assertEquals("pod-template-new", updated.getK8sPodTemplate());
    Assertions.assertEquals("/tmp/old.jar", updated.getJar());
    Assertions.assertEquals("{\"old\":true}", updated.getOptions());
    Assertions.assertEquals("old-queue", updated.getYarnQueue());
  }

  @Test
  void flinkUpdateSchemaIncludesArgsAndKubernetesPodTemplateWithoutJar() throws Exception {
    OpenAPI openAPI =
        OpenAPIController.class
            .getDeclaredMethod("flinkUpdate", Application.class, HttpServletRequest.class)
            .getDeclaredAnnotation(OpenAPI.class);

    Set<String> paramNames =
        Stream.of(openAPI.param()).map(OpenAPI.Param::name).collect(Collectors.toSet());

    Assertions.assertTrue(paramNames.contains("jobName"));
    Assertions.assertFalse(paramNames.contains("id"));
    Assertions.assertTrue(paramNames.contains("mainClass"));
    Assertions.assertTrue(paramNames.contains("flinkSql"));
    Assertions.assertTrue(paramNames.contains("args"));
    Assertions.assertTrue(paramNames.contains("dynamicProperties"));
    Assertions.assertTrue(paramNames.contains("k8sPodTemplate"));
    Assertions.assertFalse(paramNames.contains("k8sJmPodTemplate"));
    Assertions.assertFalse(paramNames.contains("k8sTmPodTemplate"));
    Assertions.assertTrue(paramNames.contains("flinkImage"));
    Assertions.assertFalse(paramNames.contains("jar"));
    Assertions.assertFalse(paramNames.contains("options"));
    Assertions.assertFalse(paramNames.contains("yarnQueue"));
  }

  @Test
  void flinkDeploySchemaIncludesKubernetesPodTemplateOnly() throws Exception {
    OpenAPI openAPI =
        OpenAPIController.class
            .getDeclaredMethod(
                "flinkDeploy", OpenAPIController.DeployRequest.class, HttpServletRequest.class)
            .getDeclaredAnnotation(OpenAPI.class);

    Set<String> paramNames =
        Stream.of(openAPI.param()).map(OpenAPI.Param::name).collect(Collectors.toSet());

    Assertions.assertTrue(paramNames.contains("srcJobName"));
    Assertions.assertTrue(paramNames.contains("dstJobName"));
    Assertions.assertTrue(paramNames.contains("mainClass"));
    Assertions.assertTrue(paramNames.contains("flinkSql"));
    Assertions.assertTrue(paramNames.contains("args"));
    Assertions.assertTrue(paramNames.contains("dynamicProperties"));
    Assertions.assertTrue(paramNames.contains("flinkImage"));
    Assertions.assertTrue(paramNames.contains("k8sPodTemplate"));
    Assertions.assertFalse(paramNames.contains("k8sJmPodTemplate"));
    Assertions.assertFalse(paramNames.contains("k8sTmPodTemplate"));
  }

  @Test
  void flinkUpdateIgnoresExplicitJobManagerAndTaskManagerPodTemplates() {
    Application request = new Application();
    request.setJobName("pod-template-job");
    request.setK8sJmPodTemplate("new-jm-template");
    request.setK8sTmPodTemplate("new-tm-template");

    Application resolved = new Application();
    resolved.setId(403L);
    resolved.setJobName("pod-template-job");
    Application existing = new Application();
    existing.setId(403L);
    existing.setJobName("pod-template-job");
    existing.setK8sJmPodTemplate("old-jm-template");
    existing.setK8sTmPodTemplate("old-tm-template");
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(resolved));
    when(applicationService.getApp(org.mockito.ArgumentMatchers.any(Application.class)))
        .thenReturn(existing);
    when(httpServletRequest.getParameterMap())
        .thenReturn(
            parameterMap(
                "jobName", "pod-template-job",
                "k8sJmPodTemplate", "new-jm-template",
                "k8sTmPodTemplate", "new-tm-template"));

    openAPIController.flinkUpdate(request, httpServletRequest);

    ArgumentCaptor<Application> updateCaptor = ArgumentCaptor.forClass(Application.class);
    verify(applicationService).update(updateCaptor.capture());
    Application merged = updateCaptor.getValue();
    Assertions.assertEquals("old-jm-template", merged.getK8sJmPodTemplate());
    Assertions.assertEquals("old-tm-template", merged.getK8sTmPodTemplate());
  }

  private Map<String, String[]> parameterMap(String... values) {
    Map<String, String[]> parameters = new HashMap<>();
    for (int i = 0; i < values.length; i += 2) {
      parameters.put(values[i], new String[] {values[i + 1]});
    }
    return parameters;
  }

  @Test
  void flinkBuildSubmitsBuildWhenNoPipelineExistsAndReturnsBuildingStatus() throws Exception {
    Application application = new Application();
    application.setId(500L);
    application.setJobName("build-job");
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(application));
    when(appBuildPipeService.getCurrentBuildPipeline(500L))
        .thenReturn(Optional.empty(), Optional.of(buildPipeline(500L, PipelineStatus.running)));
    when(applicationService.buildApplication(500L, false)).thenReturn(RestResponse.success(true));

    RestResponse response = openAPIController.flinkBuild("build-job", false);

    assertBuildResponse(response, "build-job", 500L, "BUILDING", "Build is running.");
    verify(applicationService).buildApplication(500L, false);
  }

  @Test
  void flinkBuildPollingDoesNotSubmitWhenPipelineIsRunning() throws Exception {
    Application application = new Application();
    application.setId(600L);
    application.setJobName("build-running-job");
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(application));
    when(appBuildPipeService.getCurrentBuildPipeline(600L))
        .thenReturn(Optional.of(buildPipeline(600L, PipelineStatus.running)));

    RestResponse response = openAPIController.flinkBuild("build-running-job", false);

    assertBuildResponse(response, "build-running-job", 600L, "BUILDING", "Build is running.");
    verify(appBuildPipeService).getCurrentBuildPipeline(600L);
    verify(applicationService, never())
        .buildApplication(eq(600L), org.mockito.ArgumentMatchers.anyBoolean());
  }

  @Test
  void flinkBuildForceDoesNotSubmitWhilePipelineIsRunning() throws Exception {
    Application application = new Application();
    application.setId(601L);
    application.setJobName("build-force-running-job");
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(application));
    when(appBuildPipeService.getCurrentBuildPipeline(601L))
        .thenReturn(Optional.of(buildPipeline(601L, PipelineStatus.pending)));

    RestResponse response = openAPIController.flinkBuild("build-force-running-job", true);

    assertBuildResponse(response, "build-force-running-job", 601L, "BUILDING", "Build is running.");
    verify(applicationService, never())
        .buildApplication(eq(601L), org.mockito.ArgumentMatchers.anyBoolean());
  }

  @Test
  void flinkBuildForceSubmitsAfterFailedPipeline() throws Exception {
    Application application = new Application();
    application.setId(602L);
    application.setJobName("build-failed-job");
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(application));
    when(appBuildPipeService.getCurrentBuildPipeline(602L))
        .thenReturn(
            Optional.of(buildPipeline(602L, PipelineStatus.failure)),
            Optional.of(buildPipeline(602L, PipelineStatus.running)));
    when(applicationService.buildApplication(602L, true)).thenReturn(RestResponse.success(true));

    RestResponse response = openAPIController.flinkBuild("build-failed-job", true);

    assertBuildResponse(response, "build-failed-job", 602L, "BUILDING", "Build is running.");
    verify(applicationService).buildApplication(602L, true);
  }

  @Test
  void flinkBuildReturnsFailedWhenPipelineFailedAndForceBuildIsFalse() throws Exception {
    Application application = new Application();
    application.setId(603L);
    application.setJobName("build-failed-poll-job");
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(application));
    when(appBuildPipeService.getCurrentBuildPipeline(603L))
        .thenReturn(Optional.of(buildPipeline(603L, PipelineStatus.failure)));

    RestResponse response = openAPIController.flinkBuild("build-failed-poll-job", false);

    assertBuildResponse(response, "build-failed-poll-job", 603L, "FAILED", "Build failed.");
    verify(applicationService, never())
        .buildApplication(eq(603L), org.mockito.ArgumentMatchers.anyBoolean());
  }

  @Test
  void flinkBuildReturnsCompletedWhenPipelineSucceeded() throws Exception {
    Application application = new Application();
    application.setId(604L);
    application.setJobName("build-completed-job");
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(application));
    when(appBuildPipeService.getCurrentBuildPipeline(604L))
        .thenReturn(Optional.of(buildPipeline(604L, PipelineStatus.success)));

    RestResponse response = openAPIController.flinkBuild("build-completed-job", false);

    assertBuildResponse(response, "build-completed-job", 604L, "COMPLETED", "Build completed.");
    verify(applicationService, never())
        .buildApplication(eq(604L), org.mockito.ArgumentMatchers.anyBoolean());
  }

  @Test
  void flinkBuildStatusIsNotExposedAsOpenApiEndpoint() {
    Set<String> openApiNames =
        Stream.of(OpenAPIController.class.getDeclaredMethods())
            .map(method -> method.getDeclaredAnnotation(OpenAPI.class))
            .filter(annotation -> annotation != null)
            .map(OpenAPI::name)
            .collect(Collectors.toSet());

    Set<String> postMappings =
        Stream.of(OpenAPIController.class.getDeclaredMethods())
            .map(method -> method.getDeclaredAnnotation(PostMapping.class))
            .filter(annotation -> annotation != null)
            .flatMap(annotation -> Stream.of(annotation.value()))
            .collect(Collectors.toSet());

    Assertions.assertFalse(openApiNames.contains("flinkBuildStatus"));
    Assertions.assertFalse(postMappings.contains("app/build/status"));
  }

  @SuppressWarnings("unchecked")
  private void assertBuildResponse(
      RestResponse response, String jobName, Long appId, String buildStatus, String message) {
    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Map<String, Object> data = (Map<String, Object>) response.get("data");
    Assertions.assertEquals(jobName, data.get("jobName"));
    Assertions.assertEquals(appId, data.get("appId"));
    Assertions.assertEquals(buildStatus, data.get("buildStatus"));
    Assertions.assertEquals(message, data.get("message"));
  }

  private AppBuildPipeline buildPipeline(Long appId, PipelineStatus pipelineStatus) {
    return new AppBuildPipeline().setAppId(appId).setPipeStatus(pipelineStatus);
  }

  @Test
  void flinkRestartRestoresFromLatestCheckpointAfterCancel() throws Exception {
    Application request = new Application();
    request.setJobName("restart-job");
    request.setRestoreOrTriggerSavepoint(false);
    request.setAllowNonRestored(true);
    request.setSavepointPath("file:///savepoints/sp-1");
    request.setSavepointTimeout(1L);

    Application resolved = new Application();
    resolved.setId(700L);
    resolved.setJobName("restart-job");
    Application stoppedApplication = new Application();
    stoppedApplication.setId(700L);
    stoppedApplication.setState(FlinkAppState.CANCELED.getValue());
    Savepoint checkpoint = new Savepoint();
    checkpoint.setAppId(700L);
    checkpoint.setPath("file:///checkpoints/chk-1");
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(resolved));
    when(applicationService.getById(700L)).thenReturn(stoppedApplication);
    when(savepointService.getLatest(700L)).thenReturn(checkpoint);

    RestResponse response = openAPIController.flinkRestart(request);

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
    Assertions.assertEquals("file:///checkpoints/chk-1", startParam.getSavepointPath());
  }

  @Test
  void flinkRestartSchemaRestoresFromLatestCheckpointImplicitly() throws Exception {
    OpenAPI openAPI =
        OpenAPIController.class
            .getDeclaredMethod("flinkRestart", Application.class)
            .getDeclaredAnnotation(OpenAPI.class);

    Set<String> paramNames =
        Stream.of(openAPI.param()).map(OpenAPI.Param::name).collect(Collectors.toSet());

    Assertions.assertTrue(paramNames.contains("jobName"));
    Assertions.assertTrue(paramNames.contains("allowNonRestored"));
    Assertions.assertTrue(paramNames.contains("drain"));
    Assertions.assertFalse(paramNames.contains("triggerSavepoint"));
    Assertions.assertFalse(paramNames.contains("restoreFromSavepoint"));
    Assertions.assertFalse(paramNames.contains("savepointPath"));
  }

  @Test
  void flinkStartRestoresFromLatestCheckpointWhenRequested() throws Exception {
    Application request = new Application();
    request.setJobName("start-job");
    request.setAllowNonRestored(true);
    Application resolved = new Application();
    resolved.setId(800L);
    resolved.setJobName("start-job");
    Savepoint checkpoint = new Savepoint();
    checkpoint.setAppId(800L);
    checkpoint.setPath("file:///checkpoints/chk-1");
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(resolved));
    when(savepointService.getLatest(800L)).thenReturn(checkpoint);

    OpenAPIController.StartOptions options = new OpenAPIController.StartOptions();
    options.setRestoreFromLatestCheckpoint(true);

    RestResponse response = openAPIController.flinkStart(request, options);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Assertions.assertEquals(Boolean.TRUE, response.get("data"));
    ArgumentCaptor<Application> startCaptor = ArgumentCaptor.forClass(Application.class);
    verify(applicationService).start(startCaptor.capture(), eq(false));
    Assertions.assertEquals(800L, startCaptor.getValue().getId());
    Assertions.assertTrue(startCaptor.getValue().getRestoreOrTriggerSavepoint());
    Assertions.assertTrue(startCaptor.getValue().getAllowNonRestored());
    Assertions.assertEquals("file:///checkpoints/chk-1", startCaptor.getValue().getSavepointPath());
  }

  @Test
  void copyOpenApiCurlPassesCurrentJobName() {
    when(openAPIComponent.getOpenApiCUrl("flinkGet", "http://localhost", 1L, 2L, "current-job"))
        .thenReturn("curl command");

    RestResponse response =
        openAPIController.copyOpenApiCurl("flinkGet", "http://localhost", 1L, 2L, "current-job");

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Assertions.assertEquals("curl command", response.get("data"));
    verify(openAPIComponent).getOpenApiCUrl("flinkGet", "http://localhost", 1L, 2L, "current-job");
  }

  @Test
  void flinkDeployReturnsExistingDestinationStatusWithoutRebuild() throws Exception {
    OpenAPIController.DeployRequest request = new OpenAPIController.DeployRequest();
    request.setSrcJobName("template-job");
    request.setDstJobName("existing-dst-job");
    Application destination = new Application();
    destination.setId(900L);
    destination.setJobName("existing-dst-job");
    destination.setState(FlinkAppState.ADDED.getValue());
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.singletonList(destination));

    RestResponse response = openAPIController.flinkDeploy(request, httpServletRequest);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Map<?, ?> data = (Map<?, ?>) response.get("data");
    Assertions.assertFalse(data.containsKey("deployId"));
    Assertions.assertEquals(900L, data.get("appId"));
    Assertions.assertEquals(Boolean.FALSE, data.get("buildSubmitted"));
    verify(applicationService, never()).copy(org.mockito.ArgumentMatchers.any(Application.class));
    verify(applicationService, never())
        .buildApplication(
            org.mockito.ArgumentMatchers.anyLong(), org.mockito.ArgumentMatchers.anyBoolean());
  }

  @Test
  void flinkDeployCopiesUpdatesAndBuildsMissingDestination() throws Exception {
    OpenAPIController.DeployRequest request = new OpenAPIController.DeployRequest();
    request.setSrcJobName("template-job");
    request.setDstJobName("new-dst-job");
    request.setArgs("--deploy-args");
    request.setMainClass("org.example.MainJob");
    request.setFlinkImage("registry/flink:new");
    request.setForceBuild(true);

    Application source = new Application();
    source.setId(901L);
    source.setJobName("template-job");
    source.setTeamId(100000L);
    Application copied = new Application();
    copied.setId(902L);
    copied.setJobName("new-dst-job");
    copied.setTeamId(100000L);
    copied.setFlinkSql(
        Base64.getEncoder().encodeToString("SELECT 1".getBytes(StandardCharsets.UTF_8)));
    when(applicationService.list(org.mockito.ArgumentMatchers.any()))
        .thenReturn(Collections.emptyList())
        .thenReturn(Collections.singletonList(source));
    when(applicationService.copy(org.mockito.ArgumentMatchers.any(Application.class)))
        .thenReturn(902L);
    when(applicationService.getApp(org.mockito.ArgumentMatchers.any(Application.class)))
        .thenReturn(copied);
    when(httpServletRequest.getParameterMap())
        .thenReturn(
            parameterMap(
                "srcJobName", "template-job",
                "dstJobName", "new-dst-job",
                "args", "--deploy-args",
                "mainClass", "org.example.MainJob",
                "flinkImage", "registry/flink:new",
                "forceBuild", "true"));
    when(applicationService.buildApplication(902L, true)).thenReturn(RestResponse.success(true));

    RestResponse response = openAPIController.flinkDeploy(request, httpServletRequest);

    Assertions.assertEquals(RestResponse.STATUS_SUCCESS, response.get("status"));
    Map<?, ?> data = (Map<?, ?>) response.get("data");
    Assertions.assertFalse(data.containsKey("deployId"));
    Assertions.assertEquals(902L, data.get("appId"));
    Assertions.assertEquals(Boolean.TRUE, data.get("buildSubmitted"));
    ArgumentCaptor<Application> copyCaptor = ArgumentCaptor.forClass(Application.class);
    verify(applicationService).copy(copyCaptor.capture());
    Assertions.assertEquals(901L, copyCaptor.getValue().getId());
    Assertions.assertEquals("new-dst-job", copyCaptor.getValue().getJobName());
    Assertions.assertEquals(100000L, copyCaptor.getValue().getTeamId());
    ArgumentCaptor<Application> updateCaptor = ArgumentCaptor.forClass(Application.class);
    verify(applicationService).update(updateCaptor.capture());
    Assertions.assertEquals(902L, updateCaptor.getValue().getId());
    Assertions.assertEquals("--deploy-args", updateCaptor.getValue().getArgs());
    Assertions.assertEquals("org.example.MainJob", updateCaptor.getValue().getMainClass());
    Assertions.assertEquals("registry/flink:new", updateCaptor.getValue().getFlinkImage());
    verify(applicationService).buildApplication(902L, true);
  }
}
