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
| 状态恢复 | 在任务和运行环境支持的前提下，从指定或最新 checkpoint/savepoint 恢复启动。 |
| 操作可见性 | 在应用详情页提供常用 OpenAPI curl 复制入口。 |
| 回归保障 | 为 OpenAPI 行为和 token 认证补充单元测试与测试用例文档。 |

## 非目标

| 非目标 | 原因 |
| --- | --- |
| OpenAPI Jar 上传 | 业务 Jar 预期已经内置在运行镜像或可被 Flink 运行环境访问。 |
| 用户名密码换取 OpenAPI token | 访问 token 由 StreamPark 提前创建和管理。 |
| 第二套应用生命周期实现 | OpenAPI 复用现有 StreamPark Service，避免控制台和 OpenAPI 行为分叉。 |
| 完整 savepoint 历史 OpenAPI | 当前仅暴露 latest 查询；完整历史仍保留在内部 `/flink/savepoint/history`。 |
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
| OpenAPI schema/curl 生成器 | `streampark-console-service/src/main/java/org/apache/streampark/console/core/component/OpenAPIComponent.java` | 生成 curl 示例，并自动填充应用 ID 和 Team ID 等必填参数。 |
| OpenAPI 认证 | `streampark-console-service/src/main/java/org/apache/streampark/console/system/authentication/ShiroRealm.java` | 通过“请求 credential 与数据库加密 token 解密值比较”的方式认证 OpenAPI token。 |
| OpenAPI 切面 | `streampark-console-service/src/main/java/org/apache/streampark/console/core/aspect/OpenAPIAspect.java` | 限制 OpenAPI token 只能访问带 `@OpenAPI` 的接口或白名单接口，并处理 `bindFor` 字段映射。 |
| Savepoint Controller | `streampark-console-service/src/main/java/org/apache/streampark/console/core/controller/SavepointController.java` | 补充控制台会话使用的 latest savepoint 查询能力。 |
| 应用详情页 | `streampark-console-webapp/src/views/flink/app/Detail.vue` | 展示 get、copy、update、build、start、cancel 等 Rest API 操作按钮。 |
| 请求弹窗 | `streampark-console-webapp/src/views/flink/app/components/RequestModal/index.tsx` | 请求后端生成 curl，并传递 `appId` 和 `teamId`。 |
| 文档 | `docs/flink-automation-openapi.md`、`docs/flink-automation-openapi-test-cases.md` | 描述接口、调用方式和测试用例。 |
| 测试 | `OpenAPIControllerTest.java`、`ShiroRealmTest.java` | 覆盖 Controller 委托、schema 字段、restart 流程、savepoint 方法和 token 认证。 |

## 对外接口面

所有自动化接口均使用 `POST` 和 `application/x-www-form-urlencoded` 请求体。

| 操作 | Endpoint | 所需权限 | 主要 Service 调用 |
| --- | --- | --- | --- |
| 查询应用 | `/openapi/app/get` | `app:detail` | `ApplicationService.getApp` |
| 复制应用 | `/openapi/app/copy` | `app:copy` | `ApplicationService.copy` |
| 创建应用 | `/openapi/app/create` | `app:create` | `ApplicationService.create` |
| 更新应用 | `/openapi/app/update` | `app:update` | `ApplicationService.update` |
| 构建应用 | `/openapi/app/build` | `app:create` | `ApplicationService.buildApplication` |
| 查询构建状态 | `/openapi/app/build/status` | `app:view` | `AppBuildPipeService.getCurrentBuildPipeline` |
| 启动应用 | `/openapi/app/start` | `app:start` | `ApplicationService.start` |
| 停止应用 | `/openapi/app/cancel` | `app:cancel` | `ApplicationService.cancel` |
| 重启应用 | `/openapi/app/restart` | `app:start`、`app:cancel` | `cancel`、等待、`start` |
| 触发 savepoint | `/openapi/app/savepoint/trigger` | `savepoint:trigger` | `SavepointService.trigger` |
| 查询 latest savepoint/checkpoint | `/openapi/app/savepoint/latest` | `app:view` | `SavepointService.getLatest` |

## 自动化部署流程

推荐采用“模板任务优先”的自动化流程。

```text
1. 选择模板应用 ID。
2. 调用 /openapi/app/copy，传入 id、jobName 和 teamId。
3. 调用 /openapi/app/get 查询复制后的应用详情。
4. 调用 /openapi/app/update，只提交需要修改的字段。
5. 调用 /openapi/app/build 触发构建发布。
6. 轮询 /openapi/app/build/status，直到构建完成或失败。
7. 调用 /openapi/app/start 启动任务。
8. 轮询 /openapi/app/get，直到任务进入 RUNNING 或终态失败。
9. 后续按需执行 cancel、restart、savepoint trigger 和 latest savepoint 查询。
```

`update` 请求支持局部更新。接口层会先读取当前应用详情，将请求中显式传入的字段覆盖到现有配置上，再调用 `ApplicationService.update()`。未传字段保持原值；显式传入空字符串会清空对应字段。`teamId` 不作为可更新字段，避免调用方通过局部更新改变 Team 权限上下文。

## 参数绑定策略

对外接口使用更稳定、语义更清晰的参数名，同时保留现有内部字段模型。

| 对外参数 | 内部绑定 | 说明 |
| --- | --- | --- |
| `id` | `Application.id` | get、build、start、cancel、restart、savepoint 等接口统一使用。 |
| `argument` | `Application.args` | copy/start 的可选运行参数覆盖值。 |
| `restoreFromSavepoint` | start 时绑定到 `Application.restoreOrTriggerSavepoint` | 对外表达“启动时是否恢复”。 |
| `triggerSavepoint` | cancel/restart 时绑定到 `Application.restoreOrTriggerSavepoint` | 对外表达“停止或重启前是否触发 savepoint”。 |
| `savepointPath` | `Application.savepointPath` | 根据操作语义，可以是 savepoint 或 checkpoint 路径。 |
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
  -> applicationService.start(startParam, false)
```

该接口同时要求 `app:start` 和 `app:cancel` 权限。默认等待超时时间为 60 分钟，轮询间隔为 5 秒。调用方应设置足够长的 HTTP 超时时间。

restart 启动阶段会重新构造一个只包含启动所需字段的 `Application` 对象，避免停止阶段字段泄漏到启动调用中，也让编排边界更明确。

## Savepoint 与 Checkpoint 设计

OpenAPI 提供三个状态恢复相关能力。

| 操作 | 行为 |
| --- | --- |
| `/openapi/app/savepoint/trigger` | 对运行中的应用触发 savepoint。 |
| `/openapi/app/savepoint/latest` | 返回最新记录的 `Savepoint` 对象，该记录可能代表 savepoint 或 checkpoint。 |
| `/openapi/app/start` + `restoreFromSavepoint=true` | 使用指定 `savepointPath` 启动；未传路径时尝试使用 latest 记录。 |

当前只暴露 latest 查询，不暴露完整历史列表。完整历史仍是控制台内部会话场景。使用 OpenAPI token 调用 `/flink/savepoint/history` 预期会失败，除非显式加入白名单。

savepoint/checkpoint 恢复是否成功依赖任务和运行环境。`dim-class` 测试表明，即使 API 调用被接受，自动 checkpoint 路径恢复和停止前触发 savepoint 仍可能在 Flink/Kubernetes 运行阶段失败。

## 前端 curl 生成设计

应用详情页在 Rest API 区域展示常用自动化操作按钮。

| 按钮 | Schema 名称 |
| --- | --- |
| 作业查询 | `flinkGet` |
| 作业复制 | `flinkCopy` |
| 作业更新 | `flinkUpdate` |
| 作业构建 | `flinkBuild` |
| 作业启动 | `flinkStart` |
| 作业停止 | `flinkCancel` |

请求弹窗调用 `/openapi/curl`，传入 `name`、`appId` 和 `teamId`。`OpenAPIComponent` 对 `bindFor` 为 `id`、`appId` 或 `teamId` 的必填参数自动填充当前应用上下文，确保 UI 生成的 curl 与实际应用一致。

## 错误处理

错误可能由业务逻辑、权限认证、参数校验或 Flink 运行时触发。不同层级的错误会表现为 `RestResponse` 或框架级 HTTP 错误。

| 失败场景 | 当前预期行为 |
| --- | --- |
| 缺少或无效 token | 请求被认证拦截，测试中表现为 HTTP 401。 |
| OpenAPI token 调用未开放内部接口 | 返回 `Openapi unsupported: <path>`。 |
| 复制任务名重复 | copy 失败并返回任务名重复错误。 |
| build 使用 `appId` 而不是 `id` | 参数校验失败；OpenAPI 调用方必须使用对外参数名。 |
| 查询不存在应用 ID | 当前可能返回服务端错误，后续应优化为明确的 not-found 响应。 |
| Flink 运行时恢复失败 | API 可先返回成功，但任务随后进入 `FAILED`；调用方必须轮询任务状态。 |

外部调用方应区分“API 接受请求”和“Flink 运行时操作成功”。生命周期接口调用后必须继续轮询 `/openapi/app/get` 或 `/openapi/app/build/status`。

## 测试与验证

### 自动化测试

后端聚焦验证命令：

```bash
source /etc/profile.d/java.sh && ./mvnw -pl streampark-console/streampark-console-service -DskipTests=false -Dtest=OpenAPIControllerTest,ShiroRealmTest test
```

实际结果：

```text
Tests run: 12, Failures: 0, Errors: 0, Skipped: 0
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
| get、copy、update、build、build-status、start | 通过；复制任务 `10083` 可启动到 `RUNNING` |
| 手动 savepoint trigger 和 latest 查询 | 接口接受；`10083` 返回 latest 路径 |
| 普通停止 | 通过；`10083` 可停止 |
| restart | 通过；复制任务 `10087` restart 后回到 `RUNNING`，并清理到 `CANCELED` |
| 错误 checkpoint 恢复 | 未进入 `RUNNING`；复制任务 `10085` 最终 `FAILED` |
| `dim-class` 停止前触发 savepoint | 运行时失败；复制任务 `10086` 最终 `FAILED` |
| copy 不传 `teamId` | 非预期成功，创建了 `10084`；这是已知缺口 |
| restart 权限不足测试 | 阻塞；测试环境未提供 `START_ONLY_TOKEN` |

最终清理确认：本次复制出的测试任务没有残留 `RUNNING` 状态。

## 已知风险与缺口

| 风险或缺口 | 影响 | 建议后续动作 |
| --- | --- | --- |
| copy 不传 `teamId` 仍可创建应用 | Team 权限范围校验弱于 OpenAPI schema 表达。 | 在服务调用前增加 OpenAPI 必填参数运行时校验。 |
| 查询不存在应用 ID 可能返回 NPE/server error | 外部调用方拿到泛化 500，而不是明确 not-found。 | 在 `ApplicationService.getApp` 或 Controller 包装层返回类型化 not-found 错误。 |
| 停止前触发 savepoint 依赖运行环境 | API 接受不代表 Flink savepoint 一定成功。 | 文档化运行时约束，并在可行时暴露更明确的操作状态。 |
| checkpoint 恢复可能长时间 `STARTING` 后失败 | 自动化系统可能误判恢复成功。 | 强制调用方轮询状态，并补充 Flink/Kubernetes 失败原因展示。 |
| 未暴露完整 savepoint 历史 | 外部系统无法通过 OpenAPI 选择历史恢复点。 | 如果产品需要，新增 `/openapi/app/savepoint/history`。 |

## 调用方操作建议

虽然 HTTP 接口本身是同步请求响应模式，调用方仍应把生命周期操作建模为异步工作流。

| 阶段 | 调用方要求 |
| --- | --- |
| Copy | 持久化返回的新应用 ID，后续所有步骤使用该 ID。 |
| Update | 只提交需要修改的字段；需要清空字段时显式传入空字符串。 |
| Build | 轮询 build status，直到构建流水线进入终态。 |
| Start | 轮询应用状态，直到 `RUNNING` 或终态失败。 |
| Cancel | 轮询应用状态，直到 `CANCELED`、`FAILED` 或其他终态。 |
| Restart | 设置较长 HTTP 超时时间，并在响应后继续轮询应用状态。 |
| Savepoint recovery | 将路径有效性和 Flink state 兼容性视为运行时条件，而不是只看 API 返回。 |

## 后续改进

| 优先级 | 改进项 |
| --- | --- |
| 高 | 对 `copy` 强制执行 `teamId` 运行时必填校验。 |
| 高 | 规范化应用不存在时的错误响应。 |
| 高 | 为 start/recovery/savepoint 操作补充更清晰的运行时失败诊断。 |
| 中 | 如果外部编排需要历史恢复点，新增 OpenAPI savepoint history。 |
| 中 | 增加覆盖 `OpenAPIAspect` `bindFor` 行为的集成测试。 |
| 低 | 为 OpenAPI schema 增加响应结构和生命周期轮询说明。 |
