<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Flink 自动化部署 OpenAPI 系统设计

## 背景

StreamPark 已经通过控制台 UI 和内部 `/flink/*` 接口提供完整的 Flink 应用生命周期管理能力，包括任务复制、编辑、构建、启动、停止、重启、savepoint 和 checkpoint 管理。外部平台或调度系统需要复用这些能力，但不能依赖浏览器会话、人工页面操作或控制台 Cookie。

本设计面向 Flink 任务自动化部署场景。典型流程是以已有 Flink 应用作为模板，复制出新任务，再通过 OpenAPI 修改运行配置并完成构建发布和启动。业务 Jar 默认已经在 Flink 运行镜像或任务运行环境中，因此本次 OpenAPI 能力聚焦在任务配置和生命周期控制，不覆盖 Jar 上传。

## 系统目标

本设计提供稳定的 `/openapi/app/*` 接口面，让外部系统可以自动化编排 Flink 应用部署和运维操作。

| 目标 | 说明 |
| --- | --- |
| 模板化部署 | 复制已有 Flink 应用，并在复制任务上做定制化配置。 |
| 配置自动化 | 支持更新 SQL、主类、程序参数、Dynamic Properties 和 Kubernetes Pod Template。 |
| 构建编排 | 触发 StreamPark 构建/发布流程，并轮询构建状态。 |
| 运行态控制 | 通过 OpenAPI 启动、停止和重启 Flink 应用。 |
| 状态恢复 | 在任务和运行环境支持的前提下，从 StreamPark 记录的 latest checkpoint/savepoint 恢复启动。 |
| 操作可见性 | 在应用详情页提供常用 OpenAPI curl 复制入口。 |
| 回归保障 | 为 OpenAPI 行为和 token 认证补充单元测试与测试用例文档。 |

## 非目标

| 非目标 | 原因 |
| --- | --- |
| OpenAPI Jar 上传 | 业务 Jar 预期已经内置在运行镜像或可被 Flink 运行环境访问。 |
| 用户名密码换取 OpenAPI token | 访问 token 由 StreamPark 提前创建和管理。 |
| 第二套应用生命周期实现 | OpenAPI 复用现有 StreamPark Service，避免控制台和 OpenAPI 行为分叉。 |
| 独立 savepoint/checkpoint OpenAPI | OpenAPI 不暴露 trigger/latest/history；停止前 savepoint 和启动恢复收敛到 start/cancel/restart 参数。 |
| 替换控制台 UI 行为 | 控制台内部 `/flink/*` 行为仍是现有能力的来源和参考。 |

## 总体架构

OpenAPI 层是现有 StreamPark Service 能力之上的轻量适配层。

```text
外部自动化系统
  -> /openapi/app/*
  -> Shiro JWT/OpenAPI token 认证
  -> OpenAPIAspect bindFor 参数映射
  -> OpenAPIController
  -> ApplicationService / AppBuildPipeService / SavepointService
  -> 现有 StreamPark 构建、部署、Flink、Kubernetes、savepoint 逻辑
```

生命周期语义保留在现有 Service 中。Controller 只负责定义对外契约、映射公共参数到 `Application` 字段、应用权限控制，并返回与控制台服务一致的 `RestResponse` 响应。

## 组件设计

| 组件 | 文件 | 职责 |
| --- | --- | --- |
| OpenAPI Controller | `streampark-console-service/src/main/java/org/apache/streampark/console/core/controller/OpenAPIController.java` | 暴露 `/openapi/app/*` 生命周期接口，并委托给现有 Service。 |
| OpenAPI schema/curl 生成器 | `streampark-console-service/src/main/java/org/apache/streampark/console/core/component/OpenAPIComponent.java` | 生成 curl 示例，并自动填充任务名、应用 ID 和 Team ID 等必填参数。 |
| OpenAPI 认证 | `streampark-console-service/src/main/java/org/apache/streampark/console/system/authentication/ShiroRealm.java` | 通过“请求 credential 与数据库加密 token 解密值比较”的方式认证 OpenAPI token。 |
| OpenAPI 切面 | `streampark-console-service/src/main/java/org/apache/streampark/console/core/aspect/OpenAPIAspect.java` | 限制 OpenAPI token 只能访问带 `@OpenAPI` 的接口或白名单接口，并处理 `bindFor` 字段映射。 |
| Savepoint Controller | `streampark-console-service/src/main/java/org/apache/streampark/console/core/controller/SavepointController.java` | 保留控制台会话使用的 savepoint/checkpoint 能力；OpenAPI 层不直接暴露独立 savepoint 接口。 |
| 应用详情页 | `streampark-console-webapp/src/views/flink/app/Detail.vue` | 展示 get、copy、update、build、start、cancel 等 Rest API 操作按钮。 |
| 请求弹窗 | `streampark-console-webapp/src/views/flink/app/components/RequestModal/index.tsx` | 请求后端生成 curl，并传递当前任务 `jobName`、`appId` 和 `teamId` 上下文。 |
| 文档 | `docs/flink-automation-openapi.md`、`docs/flink-automation-openapi-test-cases.md` | 描述接口、调用方式和测试用例。 |
| 测试 | `OpenAPIControllerTest.java`、`OpenAPIComponentTest.java`、`ShiroRealmTest.java` | 覆盖 Controller 委托、schema 字段、curl 生成、restart 流程和 token 认证。 |

## 对外接口面

所有自动化接口均使用 `POST` 和 `application/x-www-form-urlencoded` 请求体。

| 操作 | Endpoint | 所需权限 | 主要 Service 调用 |
| --- | --- | --- | --- |
| 查询应用 | `/openapi/app/get` | `app:detail` | `ApplicationService.getApp` |
| 复制应用 | `/openapi/app/copy` | `app:copy` | `ApplicationService.copy` |
| 一键部署 | `/openapi/app/deploy` | `app:copy`、`app:update`、`app:create` | `copy`、`update`、`buildApplication` |
| 更新应用 | `/openapi/app/update` | `app:update` | `ApplicationService.update` |
| 构建应用/查询构建状态 | `/openapi/app/build` | `app:create` | `ApplicationService.buildApplication`、`AppBuildPipeService.getCurrentBuildPipeline` |
| 启动应用 | `/openapi/app/start` | `app:start` | `ApplicationService.start` |
| 停止应用 | `/openapi/app/cancel` | `app:cancel` | `ApplicationService.cancel` |
| 重启应用 | `/openapi/app/restart` | `app:start`、`app:cancel` | `cancel`、等待、`start` |

## 自动化部署流程

推荐采用“模板任务优先”的自动化流程。

```text
1. 选择模板任务名称 srcJobName。
2. 调用 /openapi/app/deploy，传入 srcJobName 和 dstJobName。
3. 服务端在 dstJobName 不存在时复制模板、局部更新并触发构建。
4. 调用 /openapi/app/get 查询复制后的应用详情。
5. 轮询 /openapi/app/build，直到 buildStatus 为 COMPLETED 或 FAILED。
6. 调用 /openapi/app/start 启动任务；如需恢复则传 restoreFromLatestCheckpoint=true。
7. 轮询 /openapi/app/get，直到任务进入 RUNNING 或终态失败。
8. 后续按需执行 cancel 或 restart。停止前 savepoint 通过 cancel 的 triggerSavepoint 参数完成；restart 会自动从 latest checkpoint 启动。
```

OpenAPI 任务级接口使用 `jobName` 定位任务，内部解析为 `Application.id` 后复用现有 Service。`copy/deploy` 请求不接受外部传入 `teamId`。接口层会先读取源应用，并将源应用的 `teamId` 写入复制请求，避免调用方伪造 Team 权限上下文。

`update` 请求支持局部更新。接口层会先读取当前应用详情，将请求中显式传入的字段覆盖到现有配置上，再调用 `ApplicationService.update()`。未传字段保持原值；显式传入空字符串会清空对应字段。OpenAPI update 只暴露 `mainClass`、`flinkSql`、`args`、`dynamicProperties`、`flinkImage`、`k8sPodTemplate` 这组部署覆盖字段；`teamId`、执行模式、Flink 版本、Jar 路径、集群绑定、YARN 队列、告警、tags、checkpoint 失败策略等复杂控制台字段不作为 OpenAPI update/deploy 对外参数。

## 参数绑定策略

对外接口使用更稳定、语义更清晰的参数名，同时保留现有内部字段模型。

| 对外参数 | 内部绑定 | 说明 |
| --- | --- | --- |
| `jobName` | 解析为 `Application.id` | get、update、build、start、cancel、restart 等接口统一使用。 |
| `srcJobName` | 解析为源 `Application.id` | copy/deploy 的源模板任务名称。 |
| `dstJobName` | `Application.jobName` | copy/deploy 的目标任务名称。 |
| `mainClass` | `Application.mainClass` | update/deploy 可选覆盖字段，用于 Jar/custom code 主类。 |
| `flinkSql` | `Application.flinkSql` | update/deploy 可选覆盖字段，用于 SQL 内容。 |
| `args` | `Application.args` | update/deploy 可选覆盖字段，用于 Program Args。 |
| `dynamicProperties` | `Application.dynamicProperties` | update/deploy 可选覆盖字段，用于 Flink `-D` 参数。 |
| `flinkImage` | `Application.flinkImage` | update/deploy 可选覆盖字段，用于 Kubernetes Flink base image。 |
| `k8sPodTemplate` | `Application.k8sPodTemplate` | update/deploy 可选覆盖字段，用于 Kubernetes 通用 Pod Template。 |
| `argument` | `Application.args` | start 的可选运行参数覆盖值；copy 不接受参数覆盖。 |
| `restoreFromLatestCheckpoint` | start 内部查询 latest 记录后设置 `Application.restoreOrTriggerSavepoint` 和 `savepointPath` | 对外表达“启动时是否从最新 checkpoint 恢复”。 |
| `triggerSavepoint` | cancel 时绑定到 `Application.restoreOrTriggerSavepoint` | 对外表达“停止前是否触发 savepoint”。 |
| `savepointPath` | cancel 时绑定到 `Application.savepointPath` | 停止前触发 savepoint 的目录。 |
| `allowNonRestored` | `Application.allowNonRestored` | 透传给 Flink 恢复逻辑。 |
| `drain` | `Application.drain` | 控制停止前是否 drain。 |

OpenAPI token 请求会经过 `OpenAPIAspect`。切面根据 `@OpenAPI.Param(bindFor = ...)` 将公共参数写入内部字段。对于参数名与字段名一致的场景，Spring MVC 会直接完成表单参数绑定。

## 认证与授权

所有 `/openapi/*` 调用都必须通过 `Authorization` 请求头传入 OpenAPI token credential。

认证链路如下：

```text
Authorization header
  -> JWTToken
  -> ShiroRealm
  -> AccessTokenService.getByUserId
  -> 解密数据库中存储的 token
  -> 将解密后的 token 与请求 credential 比较
```

数据库中存储的 token 使用 AES-GCM 加密。AES-GCM 使用随机 IV，同一个明文每次加密后的密文都不同。因此不能通过“重新加密请求 token 后比较密文”的方式校验。当前设计改为解密数据库 token 后，与请求 credential 做明文等值比较。

授权沿用现有 Shiro 权限和 `@PermissionScope`。OpenAPI 不应绕过控制台已有权限语义。

## Restart 设计

`/openapi/app/restart` 是 Controller 层的同步编排，底层仍复用现有 Service。

```text
restart request
  -> applicationService.cancel(app)
  -> 轮询 applicationService.getById(appId)
  -> 等待 application.isCanBeStart()
  -> savepointService.getLatest(appId)
  -> applicationService.start(startParam, false)
```

该接口同时要求 `app:start` 和 `app:cancel` 权限。默认等待超时时间为 60 分钟，轮询间隔为 5 秒。调用方应设置足够长的 HTTP 超时时间。

restart 会忽略外部传入的 savepoint 触发或恢复路径，停止阶段不触发 savepoint，启动阶段自动使用 latest checkpoint 路径。启动阶段会重新构造一个只包含启动所需字段的 `Application` 对象，避免停止阶段字段泄漏到启动调用中，也让编排边界更明确。

## Savepoint 与 Checkpoint 设计

OpenAPI 不暴露独立 savepoint trigger/latest 接口。状态恢复能力收敛到 start/cancel。

| 操作 | 行为 |
| --- | --- |
| `/openapi/app/start` + `restoreFromLatestCheckpoint=true` | 内部读取 latest 记录作为恢复路径；未找到记录时启动失败。 |
| `/openapi/app/cancel` + `triggerSavepoint=true` | 停止前触发 savepoint，可选传入 savepointPath。 |
| `/openapi/app/restart` | 停止后自动读取 latest checkpoint 作为恢复路径；未找到记录时重启失败。 |

完整历史仍是控制台内部会话场景。使用 OpenAPI token 调用 `/flink/savepoint/history` 预期会失败，除非显式加入白名单。

savepoint/checkpoint 恢复是否成功依赖任务和运行环境。`dim-class` 测试表明，即使 API 调用被接受，自动 checkpoint 路径恢复和停止前触发 savepoint 仍可能在 Flink/Kubernetes 运行阶段失败。

## 前端 curl 生成设计

应用详情页在 Rest API 区域展示常用自动化操作按钮。

| 按钮 | Schema 名称 |
| --- | --- |
| 作业查询 | `flinkGet` |
| 作业复制 | `flinkCopy` |
| 一键部署 | `flinkDeploy` |
| 作业更新 | `flinkUpdate` |
| 作业构建 | `flinkBuild` |
| 作业启动 | `flinkStart` |
| 作业停止 | `flinkCancel` |
| 作业重启 | `flinkRestart` |

请求弹窗调用 `/openapi/curl`，传入 `name`、`jobName`、`appId` 和 `teamId`。`OpenAPIComponent` 对 `jobName/srcJobName/dstJobName/id/appId/teamId` 等必填参数自动填充当前应用上下文，确保 UI 生成的 curl 与实际应用一致。`dstJobName` 默认生成 `${jobName}-copy`，调用方复制后可按目标环境修改。

## 错误处理

错误可能由业务逻辑、权限认证、参数校验或 Flink 运行时触发。不同层级的错误会表现为 `RestResponse` 或框架级 HTTP 错误。

| 失败场景 | 当前预期行为 |
| --- | --- |
| 缺少或无效 token | 请求被认证拦截，测试中表现为 HTTP 401。 |
| OpenAPI token 调用未开放内部接口 | 返回 `Openapi unsupported: <path>`。 |
| 复制任务名重复 | copy 失败并返回任务名重复错误。 |
| build 使用 `appId` 而不是 `jobName` | 参数校验失败；OpenAPI 调用方必须使用对外参数名。 |
| 查询不存在 jobName | 返回明确 not-found 错误。 |
| Flink 运行时恢复失败 | API 可先返回成功，但任务随后进入 `FAILED`；调用方必须轮询任务状态。 |

外部调用方应区分“API 接受请求”和“Flink 运行时操作成功”。生命周期接口调用后必须继续轮询 `/openapi/app/get`；构建进度通过重复调用 `/openapi/app/build` 获取。

## 测试与验证

### 自动化测试

后端聚焦验证命令：

```bash
source /etc/profile.d/java.sh && ./mvnw -pl streampark-console/streampark-console-service -DskipTests=false -Dtest=OpenAPIControllerTest,OpenAPIComponentTest,ShiroRealmTest test
```

实际结果：

```text
Tests run: 27, Failures: 0, Errors: 0, Skipped: 0
BUILD SUCCESS
```

前端生产构建命令：

```bash
npm_config_strict_peer_dependencies=false npx pnpm@8.15.9 build:no-cache
```

实际结果：

```text
build successfully
```

### 测试环境结果

最近一次测试以 `dim-class` 作为模板任务。

| 项目 | 结果 |
| --- | --- |
| 模板任务 | `dim-class`，应用 ID `10073`，Team `100000` |
| Dynamic Properties 更新 | 通过；复制任务包含 `-Dparallelism.default=3` |
| get、copy、update、build 轮询、start | 通过；复制任务 `10083` 可启动到 `RUNNING` |
| 独立 OpenAPI savepoint trigger/latest | 已移除；停止前 savepoint 通过 cancel 参数完成 |
| 普通停止 | 通过；`10083` 可停止 |
| restart | 通过；复制任务 `10087` restart 后回到 `RUNNING`，并清理到 `CANCELED` |
| 错误 checkpoint 恢复 | 未进入 `RUNNING`；复制任务 `10085` 最终 `FAILED` |
| `dim-class` 停止前触发 savepoint | 运行时失败；复制任务 `10086` 最终 `FAILED` |
| copy 不传 `teamId` | 已调整为预期成功；新任务继承源任务 Team |
| restart 权限不足测试 | 阻塞；测试环境未提供 `START_ONLY_TOKEN` |

最终清理确认：本次复制出的测试任务没有残留 `RUNNING` 状态。

## 已知风险与缺口

| 风险或缺口 | 影响 | 建议后续动作 |
| --- | --- | --- |
| 查询不存在 jobName | 外部调用方无法继续生命周期操作。 | Controller 包装层返回类型化 not-found 错误。 |
| 停止前触发 savepoint 依赖运行环境 | API 接受不代表 Flink savepoint 一定成功。 | 文档化运行时约束，并在可行时暴露更明确的操作状态。 |
| checkpoint 恢复可能长时间 `STARTING` 后失败 | 自动化系统可能误判恢复成功。 | 强制调用方轮询状态，并补充 Flink/Kubernetes 失败原因展示。 |
| 未暴露完整 savepoint 历史 | 外部系统无法通过 OpenAPI 选择历史恢复点。 | 维持控制台内部能力，OpenAPI 调用方使用 latest checkpoint 启动语义。 |

## 调用方操作建议

虽然 HTTP 接口本身是同步请求响应模式，调用方仍应把生命周期操作建模为异步工作流。

| 阶段 | 调用方要求 |
| --- | --- |
| Deploy/Copy | 持久化目标任务名 `dstJobName`，后续所有任务级 OpenAPI 步骤使用该 `jobName`。 |
| Update | 只提交需要修改的字段；需要清空字段时显式传入空字符串。 |
| Build | 重复调用 `/openapi/app/build`，直到 `buildStatus` 进入 `COMPLETED` 或 `FAILED`。 |
| Start | 轮询应用状态，直到 `RUNNING` 或终态失败。 |
| Cancel | 轮询应用状态，直到 `CANCELED`、`FAILED` 或其他终态。 |
| Restart | 设置较长 HTTP 超时时间，并在响应后继续轮询应用状态。 |
| Savepoint recovery | 将路径有效性和 Flink state 兼容性视为运行时条件，而不是只看 API 返回。 |

## 后续改进

| 优先级 | 改进项 |
| --- | --- |
| 高 | 规范化应用不存在时的错误响应。 |
| 高 | 为 start/recovery/savepoint 操作补充更清晰的运行时失败诊断。 |
| 中 | 如果外部编排需要历史恢复点，新增 OpenAPI savepoint history。 |
| 中 | 增加覆盖 `OpenAPIAspect` `bindFor` 行为的集成测试。 |
| 低 | 为 OpenAPI schema 增加响应结构和生命周期轮询说明。 |
