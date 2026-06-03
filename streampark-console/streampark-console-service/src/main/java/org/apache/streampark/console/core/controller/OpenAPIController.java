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
import org.apache.streampark.console.core.annotation.PermissionScope;
import org.apache.streampark.console.core.bean.OpenAPISchema;
import org.apache.streampark.console.core.component.OpenAPIComponent;
import org.apache.streampark.console.core.entity.AppBuildPipeline;
import org.apache.streampark.console.core.entity.Application;
import org.apache.streampark.console.core.entity.Savepoint;
import org.apache.streampark.console.core.service.AppBuildPipeService;
import org.apache.streampark.console.core.service.ApplicationService;
import org.apache.streampark.console.core.service.SavepointService;

import org.apache.shiro.authz.annotation.RequiresPermissions;

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
                  "versionId",
                  "args",
                  "options",
                  "dynamicProperties",
                  "resolveOrder",
                  "executionMode",
                  "flinkImage",
                  "k8sRestExposedType",
                  "k8sPodTemplate",
                  "k8sJmPodTemplate",
                  "k8sTmPodTemplate",
                  "k8sHadoopIntegration",
                  "k8sNamespace",
                  "serviceAccount",
                  "flinkClusterId",
                  "flinkSql",
                  "sqlId",
                  "dependency",
                  "config",
                  "configId",
                  "format",
                  "description",
                  "alertId",
                  "restartSize",
                  "cpFailureAction",
                  "cpFailureRateInterval",
                  "cpMaxFailureInterval",
                  "tags",
                  "jar",
                  "mainClass",
                  "yarnQueue")));

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
            name = "id",
            description = "current flink application id",
            required = true,
            type = Long.class)
      })
  @PermissionScope(app = "#app.id")
  @PostMapping("app/get")
  @RequiresPermissions("app:detail")
  public RestResponse flinkGet(Application app) {
    return RestResponse.success(applicationService.getApp(app));
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
            name = "id",
            description = "source flink application id",
            required = true,
            type = Long.class),
        @OpenAPI.Param(
            name = "jobName",
            description = "new flink application name",
            required = true,
            type = String.class),
        @OpenAPI.Param(
            name = "teamId",
            description = "team id",
            required = true,
            type = Long.class),
        @OpenAPI.Param(
            name = "argument",
            description = "optional copied app run argument override",
            required = false,
            type = String.class,
            bindFor = "args")
      })
  @PermissionScope(app = "#app.id", team = "#app.teamId")
  @PostMapping("app/copy")
  @RequiresPermissions("app:copy")
  public RestResponse flinkCopy(Application app) throws IOException {
    Long id = applicationService.copy(app);
    Map<String, String> data = new HashMap<>();
    data.put("id", Long.toString(id));
    return id.equals(0L)
        ? RestResponse.success(false).data(data)
        : RestResponse.success(true).data(data);
  }

  @OpenAPI(
      name = "flinkCreate",
      header = {
        @OpenAPI.Param(
            name = "Authorization",
            description = "Access authorization token",
            required = true,
            type = String.class)
      },
      param = {
        @OpenAPI.Param(
            name = "teamId",
            description = "team id",
            required = true,
            type = Long.class),
        @OpenAPI.Param(
            name = "jobName",
            description = "flink application name",
            required = true,
            type = String.class),
        @OpenAPI.Param(
            name = "jobType",
            description = "1 custom code, 2 flink sql",
            required = true,
            type = Integer.class),
        @OpenAPI.Param(
            name = "executionMode",
            description = "flink execution mode",
            required = true,
            type = Integer.class),
        @OpenAPI.Param(
            name = "versionId",
            description = "flink version id",
            required = true,
            type = Long.class),
        @OpenAPI.Param(
            name = "appType",
            description = "1 StreamPark Flink, 2 Apache Flink",
            required = true,
            type = Integer.class),
        @OpenAPI.Param(
            name = "resourceFrom",
            description = "1 CICD, 2 jar in image",
            required = false,
            type = Integer.class),
        @OpenAPI.Param(
            name = "flinkSql",
            description = "flink sql content when jobType is 2",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "jar",
            description = "application jar name or path in image",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "mainClass",
            description = "application main class",
            required = false,
            type = String.class)
      })
  @PermissionScope(team = "#app.teamId")
  @PostMapping("app/create")
  @RequiresPermissions("app:create")
  public RestResponse flinkCreate(Application app) throws IOException {
    boolean saved = applicationService.create(app);
    Map<String, Object> data = new HashMap<>();
    data.put("success", saved);
    data.put("id", app.getId());
    return RestResponse.success(data);
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
            name = "id",
            description = "current flink application id",
            required = true,
            type = Long.class),
        @OpenAPI.Param(
            name = "jobName",
            description = "flink application name",
            required = false,
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
            name = "k8sPodTemplate",
            description = "kubernetes pod template",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "k8sJmPodTemplate",
            description = "kubernetes jobmanager pod template",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "k8sTmPodTemplate",
            description = "kubernetes taskmanager pod template",
            required = false,
            type = String.class)
      })
  @AppUpdated
  @PermissionScope(app = "#app.id")
  @PostMapping("app/update")
  @RequiresPermissions("app:update")
  public RestResponse flinkUpdate(Application app, HttpServletRequest request) {
    Application merged = mergeUpdateApplication(app, request);
    applicationService.update(merged);
    return RestResponse.success(true);
  }

  private Application mergeUpdateApplication(Application app, HttpServletRequest request) {
    Application query = new Application();
    query.setId(app.getId());
    Application merged = applicationService.getApp(query);
    ApiAlertException.throwIfNull(
        merged, String.format("The application id=%s not found, update failed.", app.getId()));

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
            name = "id",
            description = "current flink application id",
            required = true,
            type = Long.class),
        @OpenAPI.Param(
            name = "forceBuild",
            description = "force start build pipeline",
            required = false,
            type = Boolean.class,
            defaultValue = "false")
      })
  @PermissionScope(app = "#id")
  @PostMapping("app/build")
  @RequiresPermissions("app:create")
  public RestResponse flinkBuild(@NotNull Long id, boolean forceBuild) throws Exception {
    return applicationService.buildApplication(id, forceBuild);
  }

  @OpenAPI(
      name = "flinkBuildStatus",
      header = {
        @OpenAPI.Param(
            name = "Authorization",
            description = "Access authorization token",
            required = true,
            type = String.class)
      },
      param = {
        @OpenAPI.Param(
            name = "id",
            description = "current flink application id",
            required = true,
            type = Long.class)
      })
  @PermissionScope(app = "#id")
  @PostMapping("app/build/status")
  @RequiresPermissions("app:view")
  public RestResponse flinkBuildStatus(@NotNull Long id) {
    Optional<AppBuildPipeline> pipeline = appBuildPipeService.getCurrentBuildPipeline(id);
    return RestResponse.success(pipeline.map(AppBuildPipeline::toView).orElse(null));
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
            name = "id",
            description = "current flink application id",
            required = true,
            type = Long.class,
            bindFor = "id"),
        @OpenAPI.Param(
            name = "argument",
            description = "flink program run argument",
            required = false,
            type = String.class,
            bindFor = "args"),
        @OpenAPI.Param(
            name = "restoreFromSavepoint",
            description = "restored app from the savepoint or checkpoint",
            required = false,
            type = Boolean.class,
            defaultValue = "false",
            bindFor = "restoreOrTriggerSavepoint"),
        @OpenAPI.Param(
            name = "savepointPath",
            description = "savepoint or checkpoint path",
            required = false,
            type = String.class),
        @OpenAPI.Param(
            name = "allowNonRestored",
            description = "ignore savepoint if cannot be restored",
            required = false,
            type = Boolean.class,
            defaultValue = "false"),
      })
  @PermissionScope(app = "#app.id")
  @PostMapping("app/start")
  @RequiresPermissions("app:start")
  public RestResponse flinkStart(Application app) throws Exception {
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
            name = "id",
            description = "current flink application id",
            required = true,
            type = Long.class,
            bindFor = "id"),
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
  @PermissionScope(app = "#app.id")
  @PostMapping("app/cancel")
  @RequiresPermissions("app:cancel")
  public RestResponse flinkCancel(Application app) throws Exception {
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
            name = "id",
            description = "current flink application id",
            required = true,
            type = Long.class),
        @OpenAPI.Param(
            name = "triggerSavepoint",
            description = "trigger savepoint before restarting",
            required = false,
            type = Boolean.class,
            defaultValue = "false",
            bindFor = "restoreOrTriggerSavepoint"),
        @OpenAPI.Param(
            name = "restoreFromSavepoint",
            description = "restore restarted app from latest or given savepoint/checkpoint",
            required = false,
            type = Boolean.class,
            defaultValue = "false"),
        @OpenAPI.Param(
            name = "savepointPath",
            description = "savepoint or checkpoint path",
            required = false,
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
  @PermissionScope(app = "#app.id")
  @PostMapping("app/restart")
  @RequiresPermissions({"app:start", "app:cancel"})
  public RestResponse flinkRestart(Application app, RestartOptions options) throws Exception {
    restart(app, options.getRestoreFromSavepoint());
    return RestResponse.success(true);
  }

  @OpenAPI(
      name = "flinkSavepointTrigger",
      header = {
        @OpenAPI.Param(
            name = "Authorization",
            description = "Access authorization token",
            required = true,
            type = String.class)
      },
      param = {
        @OpenAPI.Param(
            name = "id",
            description = "current flink application id",
            required = true,
            type = Long.class),
        @OpenAPI.Param(
            name = "savepointPath",
            description = "savepoint path",
            required = false,
            type = String.class)
      })
  @PermissionScope(app = "#id")
  @PostMapping("app/savepoint/trigger")
  @RequiresPermissions("savepoint:trigger")
  public RestResponse flinkSavepointTrigger(@NotNull Long id, String savepointPath)
      throws Exception {
    savepointService.trigger(id, savepointPath);
    return RestResponse.success(true);
  }

  @OpenAPI(
      name = "flinkSavepointLatest",
      header = {
        @OpenAPI.Param(
            name = "Authorization",
            description = "Access authorization token",
            required = true,
            type = String.class)
      },
      param = {
        @OpenAPI.Param(
            name = "id",
            description = "current flink application id",
            required = true,
            type = Long.class)
      })
  @PermissionScope(app = "#id")
  @PostMapping("app/savepoint/latest")
  @RequiresPermissions("app:view")
  public RestResponse flinkSavepointLatest(@NotNull Long id) {
    Savepoint savepoint = savepointService.getLatest(id);
    return RestResponse.success(savepoint);
  }

  @PostMapping("curl")
  public RestResponse copyOpenApiCurl(
      @NotBlank String name, String baseUrl, @NotNull Long appId, @NotNull Long teamId) {
    String url = openAPIComponent.getOpenApiCUrl(name, baseUrl, appId, teamId);
    return RestResponse.success(url);
  }

  @PostMapping("schema")
  public RestResponse schema(@NotBlank(message = "{required}") String name) {
    OpenAPISchema openAPISchema = openAPIComponent.getOpenAPISchema(name);
    return RestResponse.success(openAPISchema);
  }

  private void restart(Application app, Boolean restoreFromSavepoint) throws Exception {
    Long appId = app.getId();
    applicationService.cancel(app);
    waitUntilCanStart(appId, app.getSavepointTimeout());

    Application startParam = new Application();
    startParam.setId(appId);
    boolean restore =
        restoreFromSavepoint == null
            ? Boolean.TRUE.equals(app.getRestoreOrTriggerSavepoint())
            : restoreFromSavepoint;
    startParam.setRestoreOrTriggerSavepoint(restore);
    startParam.setAllowNonRestored(Boolean.TRUE.equals(app.getAllowNonRestored()));
    if (!Boolean.TRUE.equals(app.getRestoreOrTriggerSavepoint())) {
      startParam.setSavepointPath(app.getSavepointPath());
    }
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

  public static class RestartOptions {

    private Boolean restoreFromSavepoint;

    public Boolean getRestoreFromSavepoint() {
      return restoreFromSavepoint;
    }

    public void setRestoreFromSavepoint(Boolean restoreFromSavepoint) {
      this.restoreFromSavepoint = restoreFromSavepoint;
    }
  }
}
