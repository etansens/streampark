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
import org.apache.streampark.console.core.annotation.AppUpdated;
import org.apache.streampark.console.core.annotation.OpenAPI;
import org.apache.streampark.console.core.bean.OpenAPISchema;
import org.apache.streampark.console.core.component.OpenAPIComponent;
import org.apache.streampark.console.core.entity.AppBuildPipeline;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.Savepoint;
import org.apache.streampark.console.core.service.AppBuildPipeService;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.SavepointService;
import org.apache.streampark.flink.packer.pipeline.PipelineStatus;

import org.apache.commons.lang3.StringUtils;
import org.apache.shiro.authz.annotation.RequiresPermissions;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Validated
@RestController
@RequestMapping("openapi")
public class OpenAPIController {

  private static final long DEFAULT_RESTART_TIMEOUT_MINUTES = 60L;
  private static final long RESTART_CHECK_INTERVAL_SECONDS = 5L;
  private static final Set<String> UPDATE_MERGE_FIELDS =
      Collections.unmodifiableSet(
          new HashSet<>(
              Arrays.asList(
                  "id",
                  "jobName",
                  "mainClass",
                  "flinkSql",
                  "args",
                  "dynamicProperties",
                  "flinkImage",
                  "k8sPodTemplate")));

  @Autowired private OpenAPIComponent openAPIComponent;

  @Autowired private ApplicationService applicationService;

  @Autowired private AppBuildPipeService appBuildPipeService;

  @Autowired private SavepointService savepointService;

  @OpenAPI(
      name = "flinkGet",
      header = {
        @OpenAPI.Param(
            name = "Authorization",
            description = "Access authorization token",
            required = true,
            type = String.class)
      },
      param = {
        @OpenAPI.Param(
            name = "jobName",
            description = "current flink application name",
            required = true,
            type = String.class)
      })
  @PostMapping("app/get")
  @RequiresPermissions("app:detail")
  public RestResponse flinkGet(Application app) {
    return RestResponse.success(getApplicationDetail(resolveAppByJobName(app.getJobName(), "get")));
  }

  @OpenAPI(
      name = "flinkCopy",
      header = {
        @OpenAPI.Param(
            name = "Authorization",
            description = "Access authorization token",
            required = true,
            type = String.class)
      },
      param = {
        @OpenAPI.Param(
            name = "srcJobName",
            description = "source flink application name",
            required = true,
            type = String.class),
        @OpenAPI.Param(
            name = "dstJobName",
            description = "new flink application name",
            required = true,
            type = String.class)
      })
  @PostMapping("app/copy")
  @RequiresPermissions("app:copy")
  public RestResponse flinkCopy(CopyRequest request) throws IOException {
    Application app = toCopyApplication(request);
    Long id = applicationService.copy(app);
    Map<String, String> data = new HashMap<>();
    data.put("id", Long.toString(id));
    return id.equals(0L)
        ? RestResponse.success(false).data(data)
        : RestResponse.success(true).data(data);
  }

  @OpenAPI(
      name = "flinkUpdate",
      header = {
        @OpenAPI.Param(
            name = "Authorization",
            description = "Access authorization token",
            required = true,
            type = String.class)
      },
      param = {
        @OpenAPI.Param(
            name = "jobName",
            description = "current flink application name",
            required = true,
            type = String.class),
        @OpenAPI.Param(
            name = "flinkSql",
            description = "flink sql content",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "args",
            description = "program args",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "mainClass",
            description = "application main class",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "dynamicProperties",
            description = "flink dynamic properties",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "flinkImage",
            description = "flink kubernetes base image",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "k8sPodTemplate",
            description = "kubernetes pod template",
            required = false,
            type = String.class)
      })
  @AppUpdated
  @PostMapping("app/update")
  @RequiresPermissions("app:update")
  public RestResponse flinkUpdate(Application app, HttpServletRequest request) {
    app.setId(resolveAppByJobName(app.getJobName(), "update").getId());
    Application merged = mergeUpdateApplication(app, request);
    applicationService.update(merged);
    return RestResponse.success(true);
  }

  @OpenAPI(
      name = "flinkDeploy",
      header = {
        @OpenAPI.Param(
            name = "Authorization",
            description = "Access authorization token",
            required = true,
            type = String.class)
      },
      param = {
        @OpenAPI.Param(
            name = "srcJobName",
            description = "source flink application name",
            required = true,
            type = String.class),
        @OpenAPI.Param(
            name = "dstJobName",
            description = "destination flink application name",
            required = true,
            type = String.class),
        @OpenAPI.Param(
            name = "flinkSql",
            description = "flink sql content",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "args",
            description = "program args",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "mainClass",
            description = "application main class",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "dynamicProperties",
            description = "flink dynamic properties",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "flinkImage",
            description = "flink kubernetes base image",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "k8sPodTemplate",
            description = "kubernetes pod template",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "forceBuild",
            description = "force start build pipeline",
            required = false,
            type = Boolean.class,
            defaultValue = "false")
      })
  @PostMapping("app/deploy")
  @RequiresPermissions({"app:copy", "app:update", "app:create"})
  public RestResponse flinkDeploy(DeployRequest deployRequest, HttpServletRequest request)
      throws Exception {
    Optional<Application> destination = findAppByJobName(deployRequest.getDstJobName(), "deploy");
    if (destination.isPresent()) {
      return RestResponse.success(deployResponse(deployRequest, destination.get(), false));
    }

    Application copyRequest = toCopyApplication(deployRequest);
    Long appId = applicationService.copy(copyRequest);

    Application updateRequest = deployRequest.toApplication();
    updateRequest.setId(appId);
    updateRequest.setJobName(deployRequest.getDstJobName());
    applicationService.update(mergeUpdateApplication(updateRequest, request));

    applicationService.buildApplication(appId, Boolean.TRUE.equals(deployRequest.getForceBuild()));

    Application deployed = new Application();
    deployed.setId(appId);
    deployed.setJobName(deployRequest.getDstJobName());
    return RestResponse.success(deployResponse(deployRequest, deployed, true));
  }

  private Application mergeUpdateApplication(Application app, HttpServletRequest request) {
    Application query = new Application();
    query.setId(app.getId());
    Application merged = applicationService.getApp(query);
    ApiAlertException.throwIfNull(
        merged,
        String.format("The application jobName=%s not found, update failed.", app.getJobName()));

    merged.setFlinkSql(decodeBase64(merged.getFlinkSql()));
    merged.setConfig(decodeBase64(merged.getConfig()));

    BeanWrapper source = new BeanWrapperImpl(app);
    BeanWrapper target = new BeanWrapperImpl(merged);
    for (String parameterName : request.getParameterMap().keySet()) {
      String propertyName = "argument".equals(parameterName) ? "args" : parameterName;
      if (UPDATE_MERGE_FIELDS.contains(propertyName)
          && source.isReadableProperty(propertyName)
          && target.isWritableProperty(propertyName)) {
        target.setPropertyValue(propertyName, source.getPropertyValue(propertyName));
      }
    }
    return merged;
  }

  private String decodeBase64(String value) {
    if (value == null) {
      return null;
    }
    try {
      return new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
    } catch (IllegalArgumentException e) {
      return value;
    }
  }

  private Application getApplicationDetail(Application resolved) {
    Application query = new Application();
    query.setId(resolved.getId());
    return applicationService.getApp(query);
  }

  private Application resolveAppByJobName(String jobName, String action) {
    return findAppByJobName(jobName, action)
        .orElseThrow(
            () ->
                new ApiAlertException(
                    String.format(
                        "The application jobName=%s not found, %s failed.", jobName, action)));
  }

  private Optional<Application> findAppByJobName(String jobName, String action) {
    ApiAlertException.throwIfTrue(StringUtils.isBlank(jobName), "The jobName is required.");
    List<Application> applications =
        applicationService.list(
            Wrappers.<Application>lambdaQuery().eq(Application::getJobName, jobName));
    if (applications == null || applications.isEmpty()) {
      return Optional.empty();
    }
    ApiAlertException.throwIfTrue(
        applications.size() > 1,
        String.format("The application jobName=%s is ambiguous, %s failed.", jobName, action));
    return Optional.of(applications.get(0));
  }

  private Application toCopyApplication(CopyRequest request) {
    Application source = resolveAppByJobName(request.getSrcJobName(), "copy");
    Application app = new Application();
    app.setId(source.getId());
    app.setTeamId(source.getTeamId());
    app.setJobName(request.getDstJobName());
    return app;
  }

  private Map<String, Object> deployResponse(
      DeployRequest deployRequest, Application application, boolean buildSubmitted) {
    Map<String, Object> data = new HashMap<>();
    data.put("srcJobName", deployRequest.getSrcJobName());
    data.put("dstJobName", deployRequest.getDstJobName());
    data.put("appId", application.getId());
    data.put("state", application.getState());
    data.put("buildSubmitted", buildSubmitted);
    data.put("message", buildSubmitted ? "Build submitted." : "Application already exists.");
    return data;
  }

  @OpenAPI(
      name = "flinkBuild",
      header = {
        @OpenAPI.Param(
            name = "Authorization",
            description = "Access authorization token",
            required = true,
            type = String.class)
      },
      param = {
        @OpenAPI.Param(
            name = "jobName",
            description = "current flink application name",
            required = true,
            type = String.class),
        @OpenAPI.Param(
            name = "forceBuild",
            description = "force start build pipeline",
            required = false,
            type = Boolean.class,
            defaultValue = "false")
      })
  @PostMapping("app/build")
  @RequiresPermissions("app:create")
  public RestResponse flinkBuild(@NotBlank String jobName, boolean forceBuild) throws Exception {
    Application application = resolveAppByJobName(jobName, "build");
    Optional<AppBuildPipeline> pipeline =
        appBuildPipeService.getCurrentBuildPipeline(application.getId());
    if (shouldSubmitBuild(application, pipeline, forceBuild)) {
      applicationService.buildApplication(application.getId(), forceBuild);
      pipeline = appBuildPipeService.getCurrentBuildPipeline(application.getId());
    }
    return RestResponse.success(buildResponse(application, pipeline));
  }

  private boolean shouldSubmitBuild(
      Application application, Optional<AppBuildPipeline> pipeline, boolean forceBuild) {
    if (!pipeline.isPresent()) {
      return true;
    }
    PipelineStatus status = pipeline.get().getPipelineStatus();
    if (PipelineStatus.pending == status || PipelineStatus.running == status) {
      return false;
    }
    return forceBuild
        || (PipelineStatus.success == status && Boolean.TRUE.equals(application.getBuild()));
  }

  private Map<String, Object> buildResponse(
      Application application, Optional<AppBuildPipeline> pipeline) {
    String buildStatus = openApiBuildStatus(pipeline);
    Map<String, Object> data = new HashMap<>();
    data.put("jobName", application.getJobName());
    data.put("appId", application.getId());
    data.put("buildStatus", buildStatus);
    data.put("message", buildStatusMessage(buildStatus));
    return data;
  }

  private String openApiBuildStatus(Optional<AppBuildPipeline> pipeline) {
    if (!pipeline.isPresent()) {
      return "COMPLETED";
    }
    PipelineStatus status = pipeline.get().getPipelineStatus();
    if (PipelineStatus.failure == status) {
      return "FAILED";
    }
    if (PipelineStatus.success == status) {
      return "COMPLETED";
    }
    return "BUILDING";
  }

  private String buildStatusMessage(String buildStatus) {
    if ("FAILED".equals(buildStatus)) {
      return "Build failed.";
    }
    if ("COMPLETED".equals(buildStatus)) {
      return "Build completed.";
    }
    return "Build is running.";
  }

  @OpenAPI(
      name = "flinkStart",
      header = {
        @OpenAPI.Param(
            name = "Authorization",
            description = "Access authorization token",
            required = true,
            type = String.class)
      },
      param = {
        @OpenAPI.Param(
            name = "jobName",
            description = "current flink application name",
            required = true,
            type = String.class),
        @OpenAPI.Param(
            name = "argument",
            description = "flink program run argument",
            required = false,
            type = String.class,
            bindFor = "args"),
        @OpenAPI.Param(
            name = "restoreFromLatestCheckpoint",
            description = "restore app from the latest checkpoint recorded by StreamPark",
            required = false,
            type = Boolean.class,
            defaultValue = "false"),
        @OpenAPI.Param(
            name = "allowNonRestored",
            description = "ignore savepoint if cannot be restored",
            required = false,
            type = Boolean.class,
            defaultValue = "false"),
      })
  @PostMapping("app/start")
  @RequiresPermissions("app:start")
  public RestResponse flinkStart(Application app, StartOptions options) throws Exception {
    app.setId(resolveAppByJobName(app.getJobName(), "start").getId());
    if (Boolean.TRUE.equals(options.getRestoreFromLatestCheckpoint())) {
      Savepoint latest = savepointService.getLatest(app.getId());
      ApiAlertException.throwIfTrue(
          latest == null || StringUtils.isBlank(latest.getPath()),
          String.format(
              "The application jobName=%s has no available checkpoint, start failed.",
              app.getJobName()));
      app.setRestoreOrTriggerSavepoint(true);
      app.setSavepointPath(latest.getPath());
    }
    applicationService.start(app, false);
    return RestResponse.success(true);
  }

  @OpenAPI(
      name = "flinkCancel",
      header = {
        @OpenAPI.Param(
            name = "Authorization",
            description = "Access authorization token",
            required = true,
            type = String.class)
      },
      param = {
        @OpenAPI.Param(
            name = "jobName",
            description = "current flink application name",
            required = true,
            type = String.class),
        @OpenAPI.Param(
            name = "triggerSavepoint",
            description = "trigger savepoint before taking stopping",
            required = false,
            type = Boolean.class,
            defaultValue = "false",
            bindFor = "restoreOrTriggerSavepoint"),
        @OpenAPI.Param(
            name = "savepointPath",
            description = "savepoint path",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "drain",
            description = "send max watermark before canceling",
            required = false,
            type = Boolean.class,
            defaultValue = "false"),
      })
  @PostMapping("app/cancel")
  @RequiresPermissions("app:cancel")
  public RestResponse flinkCancel(Application app) throws Exception {
    app.setId(resolveAppByJobName(app.getJobName(), "cancel").getId());
    applicationService.cancel(app);
    return RestResponse.success();
  }

  @OpenAPI(
      name = "flinkRestart",
      header = {
        @OpenAPI.Param(
            name = "Authorization",
            description = "Access authorization token",
            required = true,
            type = String.class)
      },
      param = {
        @OpenAPI.Param(
            name = "jobName",
            description = "current flink application name",
            required = true,
            type = String.class),
        @OpenAPI.Param(
            name = "allowNonRestored",
            description = "ignore savepoint state if cannot be restored",
            required = false,
            type = Boolean.class,
            defaultValue = "false"),
        @OpenAPI.Param(
            name = "drain",
            description = "send max watermark before canceling",
            required = false,
            type = Boolean.class,
            defaultValue = "false")
      })
  @PostMapping("app/restart")
  @RequiresPermissions({"app:start", "app:cancel"})
  public RestResponse flinkRestart(Application app) throws Exception {
    app.setId(resolveAppByJobName(app.getJobName(), "restart").getId());
    restart(app);
    return RestResponse.success(true);
  }

  @PostMapping("curl")
  public RestResponse copyOpenApiCurl(
      @NotBlank String name,
      String baseUrl,
      @NotNull Long appId,
      @NotNull Long teamId,
      @NotBlank String jobName) {
    String url = openAPIComponent.getOpenApiCUrl(name, baseUrl, appId, teamId, jobName);
    return RestResponse.success(url);
  }

  @PostMapping("schema")
  public RestResponse schema(@NotBlank(message = "{required}") String name) {
    OpenAPISchema openAPISchema = openAPIComponent.getOpenAPISchema(name);
    return RestResponse.success(openAPISchema);
  }

  private void restart(Application app) throws Exception {
    Long appId = app.getId();
    app.setRestoreOrTriggerSavepoint(false);
    app.setSavepointPath(null);
    applicationService.cancel(app);
    waitUntilCanStart(appId, app.getSavepointTimeout());

    Savepoint latest = savepointService.getLatest(appId);
    ApiAlertException.throwIfTrue(
        latest == null || StringUtils.isBlank(latest.getPath()),
        String.format(
            "The application jobName=%s has no available checkpoint, restart failed.",
            app.getJobName()));

    Application startParam = new Application();
    startParam.setId(appId);
    startParam.setRestoreOrTriggerSavepoint(true);
    startParam.setAllowNonRestored(Boolean.TRUE.equals(app.getAllowNonRestored()));
    startParam.setSavepointPath(latest.getPath());
    applicationService.start(startParam, false);
  }

  private void waitUntilCanStart(Long appId, Long timeoutMinutes) throws InterruptedException {
    long timeout = timeoutMinutes == null ? DEFAULT_RESTART_TIMEOUT_MINUTES : timeoutMinutes;
    long deadline = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(timeout);
    while (System.currentTimeMillis() < deadline) {
      Application application = applicationService.getById(appId);
      if (application != null && application.isCanBeStart()) {
        return;
      }
      TimeUnit.SECONDS.sleep(RESTART_CHECK_INTERVAL_SECONDS);
    }
    throw new IllegalStateException(
        String.format("Timed out waiting application %s to stop before restart", appId));
  }

  public static class CopyRequest {

    private String srcJobName;

    private String dstJobName;

    public String getSrcJobName() {
      return srcJobName;
    }

    public void setSrcJobName(String srcJobName) {
      this.srcJobName = srcJobName;
    }

    public String getDstJobName() {
      return dstJobName;
    }

    public void setDstJobName(String dstJobName) {
      this.dstJobName = dstJobName;
    }
  }

  public static class DeployRequest extends CopyRequest {

    private String flinkSql;

    private String args;

    private String mainClass;

    private String dynamicProperties;

    private String flinkImage;

    private String k8sPodTemplate;

    private Boolean forceBuild;

    public Application toApplication() {
      Application app = new Application();
      app.setFlinkSql(flinkSql);
      app.setArgs(args);
      app.setMainClass(mainClass);
      app.setDynamicProperties(dynamicProperties);
      app.setFlinkImage(flinkImage);
      app.setK8sPodTemplate(k8sPodTemplate);
      return app;
    }

    public String getFlinkSql() {
      return flinkSql;
    }

    public void setFlinkSql(String flinkSql) {
      this.flinkSql = flinkSql;
    }

    public String getArgs() {
      return args;
    }

    public void setArgs(String args) {
      this.args = args;
    }

    public String getMainClass() {
      return mainClass;
    }

    public void setMainClass(String mainClass) {
      this.mainClass = mainClass;
    }

    public String getDynamicProperties() {
      return dynamicProperties;
    }

    public void setDynamicProperties(String dynamicProperties) {
      this.dynamicProperties = dynamicProperties;
    }

    public String getFlinkImage() {
      return flinkImage;
    }

    public void setFlinkImage(String flinkImage) {
      this.flinkImage = flinkImage;
    }

    public String getK8sPodTemplate() {
      return k8sPodTemplate;
    }

    public void setK8sPodTemplate(String k8sPodTemplate) {
      this.k8sPodTemplate = k8sPodTemplate;
    }

    public Boolean getForceBuild() {
      return forceBuild;
    }

    public void setForceBuild(Boolean forceBuild) {
      this.forceBuild = forceBuild;
    }
  }

  public static class StartOptions {

    private Boolean restoreFromLatestCheckpoint;

    public Boolean getRestoreFromLatestCheckpoint() {
      return restoreFromLatestCheckpoint;
    }

    public void setRestoreFromLatestCheckpoint(Boolean restoreFromLatestCheckpoint) {
      this.restoreFromLatestCheckpoint = restoreFromLatestCheckpoint;
    }
  }
}
