<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one or more
  ~ contributor license agreements.  See the NOTICE file distributed with
  ~ this work for additional information regarding copyright ownership.
  ~ The ASF licenses this file to You under the Apache License, Version 2.0
  ~ (the "License"); you may not use this file except in compliance with
  ~ the License.  You may obtain a copy of the License at
  ~
  ~    http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software
  ~ distributed under the License is distributed on an "AS IS" BASIS,
  ~ WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  ~ See the License for the specific language governing permissions and
  ~ limitations under the License.
  ~
  -->

# Flink 任务全周期自动化管理 OpenAPI

本文档说明 StreamPark Console 中 Flink 任务全生命周期管理相关 OpenAPI，覆盖：**创建、编译、启动、停止、重启**。

## 1. 认证与通用约定

- 所有接口基于 `OpenAPIController`，统一前缀：`/openapi`。
- 认证方式：在请求头中传入 `Authorization`（OpenAPI Access Token）。
- 请求方法均为 `POST`。
- 请求参数默认为表单参数（`application/x-www-form-urlencoded`）。
- 返回结构为 StreamPark 统一 `RestResponse`。

示例请求头：

```http
Authorization: Bearer <your-openapi-token>
```

## 2. 接口总览

| 生命周期阶段 | API 名称 | 路径 | 说明 |
|---|---|---|---|
| 创建 | flinkCreate | `/openapi/app/create` | 创建 Flink 应用 |
| 编译/构建 | flinkBuild | `/openapi/app/build` | 触发应用构建流水线 |
| 启动 | flinkStart | `/openapi/app/start` | 启动 Flink 应用 |
| 停止 | flinkCancel | `/openapi/app/cancel` | 停止 Flink 应用 |
| 重启 | flinkRestart | `/openapi/app/restart` | 重启 Flink 应用 |

## 3. 详细接口说明

### 3.1 创建任务（flinkCreate）

- **URL**: `POST /openapi/app/create`
- **权限**: `app:create`
- **作用域**: `team` 维度（`teamId`）
- **参数**（关键字段）:
  - `teamId`（必填）：团队 ID
  - `jobName`（必填）：任务名称
  - `appType`（必填）：应用类型
  - `jobType`（必填）：任务类型（1: custom code, 2: flink sql）
  - `executionMode`（必填）：执行模式
  - `projectId`（可选）：项目 ID
  - `description`（可选）：任务描述

示例：

```bash
curl -X POST 'http://<streampark-host>/openapi/app/create' \
  -H 'Authorization: Bearer <token>' \
  -d 'teamId=100000&jobName=orders-realtime-job&appType=2&jobType=1&executionMode=4&projectId=200001&description=created-by-openapi'
```

成功返回（示意）：

```json
{
  "status": "success",
  "data": {
    "id": 12345
  }
}
```

---

### 3.2 编译/构建任务（flinkBuild）

- **URL**: `POST /openapi/app/build`
- **权限**: `app:create`
- **作用域**: `app` 维度（`appId`）
- **参数**:
  - `id`（必填）：应用 ID
  - `forceBuild`（可选，默认 `false`）：是否强制触发构建

示例：

```bash
curl -X POST 'http://<streampark-host>/openapi/app/build' \
  -H 'Authorization: Bearer <token>' \
  -d 'id=12345&forceBuild=true'
```

---

### 3.3 启动任务（flinkStart）

- **URL**: `POST /openapi/app/start`
- **权限**: `app:start`
- **作用域**: `app` 维度（`id`）
- **参数**:
  - `id`（必填）：应用 ID
  - `argument`（可选）：运行参数
  - `restoreFromSavepoint`（可选，默认 `false`）：是否从 savepoint/checkpoint 恢复
  - `savepointPath`（可选）：savepoint/checkpoint 路径
  - `allowNonRestored`（可选，默认 `false`）：是否允许部分状态无法恢复

示例：

```bash
curl -X POST 'http://<streampark-host>/openapi/app/start' \
  -H 'Authorization: Bearer <token>' \
  -d 'id=12345&argument=--env prod&restoreFromSavepoint=false'
```

---

### 3.4 停止任务（flinkCancel）

- **URL**: `POST /openapi/app/cancel`
- **权限**: `app:cancel`
- **作用域**: `app` 维度（`id`）
- **参数**:
  - `id`（必填）：应用 ID
  - `triggerSavepoint`（可选，默认 `false`）：停止前是否触发 savepoint
  - `savepointPath`（可选）：savepoint 存储路径
  - `drain`（可选，默认 `false`）：取消前是否发送最大 watermark

示例：

```bash
curl -X POST 'http://<streampark-host>/openapi/app/cancel' \
  -H 'Authorization: Bearer <token>' \
  -d 'id=12345&triggerSavepoint=true&drain=false'
```

---

### 3.5 重启任务（flinkRestart）

- **URL**: `POST /openapi/app/restart`
- **权限**: `app:start`
- **作用域**: `app` 维度（`id`）
- **参数**:
  - `id`（必填）：应用 ID

示例：

```bash
curl -X POST 'http://<streampark-host>/openapi/app/restart' \
  -H 'Authorization: Bearer <token>' \
  -d 'id=12345'
```

## 4. 推荐调用顺序（自动化场景）

1. `flinkCreate`：创建应用
2. `flinkBuild`：构建产物
3. `flinkStart`：启动运行
4. 运维阶段按需调用 `flinkCancel` 或 `flinkRestart`

## 5. OpenAPI 元数据与动态查询

除生命周期接口外，还可使用以下通用接口获取定义信息：

- `POST /openapi/schema`：按 API 名称获取参数 schema
- `POST /openapi/curl`：生成对应 API 的 curl 模板

示例：

```bash
curl -X POST 'http://<streampark-host>/openapi/schema' \
  -H 'Authorization: Bearer <token>' \
  -d 'name=flinkStart'
```

```bash
curl -X POST 'http://<streampark-host>/openapi/curl' \
  -H 'Authorization: Bearer <token>' \
  -d 'name=flinkStart&appId=12345&teamId=100000'
```
