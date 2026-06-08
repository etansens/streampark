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

# Flink Automation OpenAPI

本文档说明 StreamPark Console 面向 Flink 自动化部署的 `/openapi/*` 接口。接口用于从模板任务复制、覆盖少量部署参数、构建、启动、停止和重启 Flink 任务。

## 快速约定

```bash
export BASE_URL="http://localhost:10000"
export TOKEN="replace-with-streampark-access-token"
export SRC_JOB_NAME="template-job"
export DST_JOB_NAME="target-job"
export JOB_NAME="target-job"
```

所有 OpenAPI 请求都需要认证头：

```bash
-H "Authorization: ${TOKEN}"
```

请求体统一使用 `application/x-www-form-urlencoded`。不要发送 JSON body。

```bash
curl -X POST "${BASE_URL}/openapi/app/get" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${JOB_NAME}"
```

## 总体合同

### 任务定位

任务级接口统一使用 `jobName` 定位任务。服务端会把 `jobName` 解析为内部 `Application.id` 后复用现有 Service。

| 场景 | 行为 |
| --- | --- |
| `jobName` 为空 | 返回失败，提示 `The jobName is required.` |
| 找不到任务 | 返回失败，提示对应操作失败。 |
| 同名任务超过 1 个 | 返回 ambiguous 失败。 |

自动化调用方应保证任务名唯一。

### 请求与响应

所有接口返回 StreamPark 统一 `RestResponse`。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `status` | string | 成功为 `success`，失败为 `error`。 |
| `code` | number | 成功为 `200`；失败时为服务端错误码。 |
| `data` | any | 接口业务数据。部分接口只返回布尔值或不返回该字段。 |
| `message` | string | 错误说明，或部分接口的业务提示。 |

### 部署覆盖字段

`/openapi/app/update` 和 `/openapi/app/deploy` 只对外开放这组部署覆盖字段：

| 字段 | 说明 |
| --- | --- |
| `mainClass` | Jar/custom code 任务主类。 |
| `flinkSql` | Flink SQL 内容，提交时使用明文。 |
| `args` | Program Args。 |
| `dynamicProperties` | Flink `-D` 动态参数。可能包含敏感配置，不要直接打印到日志。 |
| `flinkImage` | Kubernetes Application 模式使用的 Flink base image。 |
| `k8sPodTemplate` | Kubernetes 通用 Pod Template YAML。 |

未传字段保留原值；显式传入空字符串会清空该字段。

OpenAPI update/deploy 只处理上表列出的字段。请求中包含其他字段时，服务端会忽略。

### 构建状态

`/openapi/app/build` 同时用于提交构建和轮询构建状态。

对外只暴露三种状态：

| 状态 | 说明 |
| --- | --- |
| `BUILDING` | 内部 pipeline 为 pending/running，或刚提交构建。 |
| `COMPLETED` | 内部 pipeline 成功，或当前没有需要等待的构建。 |
| `FAILED` | 内部 pipeline 失败。 |

### Checkpoint 与 Savepoint

OpenAPI 不暴露独立 savepoint trigger/latest 接口。

| 操作 | 行为 |
| --- | --- |
| `start` + `restoreFromLatestCheckpoint=true` | 服务端读取 latest checkpoint/savepoint 记录作为恢复路径。 |
| `cancel` + `triggerSavepoint=true` | 停止前触发 savepoint。 |
| `restart` | 停止后自动读取 latest checkpoint/savepoint 记录启动。 |

## 推荐流程

1. 调用 `/openapi/app/deploy`，从模板任务复制目标任务、覆盖部署字段并触发构建。
2. 重复调用 `/openapi/app/build`，直到 `buildStatus` 为 `COMPLETED` 或 `FAILED`。
3. 按需调用 `/openapi/app/get` 查询任务详情。
4. 调用 `/openapi/app/start` 启动任务，可选择从 latest checkpoint 恢复。
5. 调用 `/openapi/app/cancel` 停止任务，可选择停止前触发 savepoint。
6. 调用 `/openapi/app/restart` 执行停止、等待、从 latest checkpoint 启动的同步编排。

最短部署示例：

```bash
curl -X POST "${BASE_URL}/openapi/app/deploy" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "srcJobName=${SRC_JOB_NAME}" \
  --data-urlencode "dstJobName=${DST_JOB_NAME}" \
  --data-urlencode "mainClass=org.example.MainJob" \
  --data-urlencode "flinkImage=registry.example.com/flink/job-runtime:20260603" \
  --data-urlencode "args=--env prod" \
  --data-urlencode "forceBuild=false"

curl -X POST "${BASE_URL}/openapi/app/build" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${DST_JOB_NAME}" \
  --data-urlencode "forceBuild=false"
```

## 接口总览

| 操作 | OpenAPI 接口 | 说明 |
| --- | --- | --- |
| 查询任务 | `POST /openapi/app/get` | 按 `jobName` 查询任务详情。 |
| 复制任务 | `POST /openapi/app/copy` | 从模板任务复制目标任务，不做参数覆盖。 |
| 一键部署 | `POST /openapi/app/deploy` | copy + update overlay + build，不启动任务。 |
| 更新任务 | `POST /openapi/app/update` | 仅覆盖部署字段，不构建、不启动。 |
| 构建/查询构建 | `POST /openapi/app/build` | 提交构建或轮询三态构建状态。 |
| 启动任务 | `POST /openapi/app/start` | 启动任务，可从 latest checkpoint 恢复。 |
| 停止任务 | `POST /openapi/app/cancel` | 停止任务，可先触发 savepoint。 |
| 重启任务 | `POST /openapi/app/restart` | 停止后等待可启动，再从 latest checkpoint 启动。 |

## 接口详情

### 1. 查询任务

`POST /openapi/app/get`

按 `jobName` 查询完整任务详情。返回 `Application` 明细。

**请求参数**

| 参数 | 必填 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `jobName` | 是 | string | 无 | Flink 任务名称，必须唯一定位一个任务。 |

**响应字段**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `data.id` | number | StreamPark 内部任务 ID。 |
| `data.jobName` | string | 任务名称。 |
| `data.teamId` | number | 任务所属 Team ID。 |
| `data.state` | number | 任务运行状态，取值来自 `FlinkAppState`。 |
| `data.flinkSql` | string | SQL 任务内容，当前返回值可能为 Base64 编码。 |
| `data.args` | string | Program Args。 |
| `data.dynamicProperties` | string | Flink `-D` 动态参数，可能包含敏感配置。 |
| `data.flinkImage` | string | Kubernetes Application 模式使用的 Flink base image。 |

**示例**

```bash
curl -X POST "${BASE_URL}/openapi/app/get" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${JOB_NAME}"
```

```json
{
  "status": "success",
  "code": 200,
  "data": {
    "id": 10086,
    "jobName": "target-job",
    "teamId": 100000,
    "state": 0,
    "args": "--env prod",
    "dynamicProperties": "-Dexecution.checkpointing.interval=30000",
    "flinkImage": "registry.example.com/flink/job-runtime:20260603"
  }
}
```

### 2. 复制任务

`POST /openapi/app/copy`

从模板任务复制一个新任务。copy 只负责创建目标任务，不接受 `args`、`mainClass`、`flinkSql` 等参数覆盖。参数覆盖请使用 `/openapi/app/update` 或 `/openapi/app/deploy`。

**请求参数**

| 参数 | 必填 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `srcJobName` | 是 | string | 无 | 源模板任务名称，必须唯一定位一个已有任务。 |
| `dstJobName` | 是 | string | 无 | 新任务名称，不能与现有任务重复。 |

**响应字段**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `data.id` | string | 新任务的内部任务 ID；服务端按字符串返回。 |

**业务逻辑**

服务端根据 `srcJobName` 找到源任务，读取源任务的 `id` 和 `teamId`，把 `dstJobName` 设置为新任务名后调用现有复制服务。调用方不需要也不能传 `teamId`。

**示例**

```bash
curl -X POST "${BASE_URL}/openapi/app/copy" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "srcJobName=${SRC_JOB_NAME}" \
  --data-urlencode "dstJobName=${DST_JOB_NAME}"
```

```json
{
  "status": "success",
  "code": 200,
  "data": {
    "id": "10086"
  }
}
```

### 3. 一键部署

`POST /openapi/app/deploy`

从 `srcJobName` 复制到 `dstJobName`，对新任务执行局部覆盖并触发构建。deploy 不启动任务。

`dstJobName` 同时作为幂等部署 ID。目标任务已存在时，接口直接返回目标任务状态，不重复 copy、update 或 build。

**请求参数**

| 参数 | 必填 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `srcJobName` | 是 | string | 无 | 源模板任务名称，必须唯一定位一个已有任务。 |
| `dstJobName` | 是 | string | 无 | 目标任务名称，同时作为幂等部署 ID。 |
| `mainClass` | 否 | string | 保留模板值 | Jar/custom code 任务主类。 |
| `flinkSql` | 否 | string | 保留模板值 | SQL 内容，提交时使用明文。 |
| `args` | 否 | string | 保留模板值 | Program Args。 |
| `dynamicProperties` | 否 | string | 保留模板值 | Flink `-D` 动态参数。 |
| `flinkImage` | 否 | string | 保留模板值 | Kubernetes Application 模式使用的 Flink base image。 |
| `k8sPodTemplate` | 否 | string | 保留模板值 | Kubernetes 通用 Pod Template YAML。 |
| `forceBuild` | 否 | boolean | `false` | 是否强制构建。构建中不会并发提交第二个构建。 |

**响应字段**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `data.srcJobName` | string | 源模板任务名称。 |
| `data.dstJobName` | string | 目标任务名称。 |
| `data.appId` | number | 目标任务内部 ID。 |
| `data.state` | number | 目标任务状态。新复制任务可能为空。 |
| `data.buildSubmitted` | boolean | 本次调用是否提交了构建。目标已存在时为 `false`。 |
| `data.message` | string | `Build submitted.` 或 `Application already exists.`。 |

**示例**

```bash
curl -X POST "${BASE_URL}/openapi/app/deploy" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "srcJobName=${SRC_JOB_NAME}" \
  --data-urlencode "dstJobName=${DST_JOB_NAME}" \
  --data-urlencode "mainClass=org.example.MainJob" \
  --data-urlencode "flinkImage=registry.example.com/flink/job-runtime:20260603" \
  --data-urlencode "args=--env prod" \
  --data-urlencode "forceBuild=false"
```

```json
{
  "status": "success",
  "code": 200,
  "data": {
    "srcJobName": "template-job",
    "dstJobName": "target-job",
    "appId": 10086,
    "state": null,
    "buildSubmitted": true,
    "message": "Build submitted."
  }
}
```

### 4. 更新任务

`POST /openapi/app/update`

更新任务的部署覆盖字段。update 不触发构建，不启动任务。

**请求参数**

| 参数 | 必填 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `jobName` | 是 | string | 无 | Flink 任务名称，必须唯一定位一个任务。 |
| `mainClass` | 否 | string | 保留原值 | Jar/custom code 任务主类。 |
| `flinkSql` | 否 | string | 保留原值 | SQL 内容，提交时使用明文。 |
| `args` | 否 | string | 保留原值 | Program Args。 |
| `dynamicProperties` | 否 | string | 保留原值 | Flink `-D` 动态参数。 |
| `flinkImage` | 否 | string | 保留原值 | Kubernetes Application 模式使用的 Flink base image。 |
| `k8sPodTemplate` | 否 | string | 保留原值 | Kubernetes 通用 Pod Template YAML。 |

**响应字段**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `data` | boolean | 更新成功时为 `true`。 |

**业务逻辑**

服务端根据 `jobName` 解析内部任务 ID，读取当前任务详情，再只合并本次请求中显式出现且位于允许列表的字段。

`argument` 不是 update 的有效别名。更新 Program Args 请使用 `args`。

**示例**

```bash
curl -X POST "${BASE_URL}/openapi/app/update" \
  -H "Authorization: ${TOKEN}" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "jobName=${JOB_NAME}" \
  --data-urlencode "mainClass=org.example.MainJob" \
  --data-urlencode "flinkSql=CREATE TABLE source_table (id INT) WITH ('connector'='datagen'); CREATE TABLE sink_table (id INT) WITH ('connector'='print'); INSERT INTO sink_table SELECT id FROM source_table;" \
  --data-urlencode "args=--env test --parallelism 2" \
  --data-urlencode "dynamicProperties=-Dexecution.checkpointing.interval=30000" \
  --data-urlencode "flinkImage=registry.example.com/flink/job-runtime:20260603" \
  --data-urlencode "k8sPodTemplate=${POD_TEMPLATE_YAML}"
```

```json
{
  "status": "success",
  "code": 200,
  "data": true
}
```

### 5. 构建/查询构建状态

`POST /openapi/app/build`

提交构建或查询当前构建状态。调用方可重复调用该接口轮询构建进度。

该接口不做服务端长等待，也没有构建超时参数。每次请求只提交或查询一次状态，调用方需要自行设置轮询间隔和总等待时间。

**请求参数**

| 参数 | 必填 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `jobName` | 是 | string | 无 | Flink 任务名称，必须唯一定位一个任务。 |
| `forceBuild` | 否 | boolean | `false` | 是否显式重新构建。 |

**`forceBuild` 行为**

| 值 | 行为 |
| --- | --- |
| `false` | 无当前构建时提交构建；构建中不重复提交；失败时只返回失败状态。 |
| `true` | 显式重新构建；如果已有构建正在进行，不并发提交第二个构建。 |

**响应字段**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `data.jobName` | string | 任务名称。 |
| `data.appId` | number | 内部任务 ID。 |
| `data.buildStatus` | string | `BUILDING`、`COMPLETED` 或 `FAILED`。 |
| `data.message` | string | 构建状态说明。 |

**示例**

```bash
curl -X POST "${BASE_URL}/openapi/app/build" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${JOB_NAME}" \
  --data-urlencode "forceBuild=false"
```

```json
{
  "status": "success",
  "code": 200,
  "data": {
    "jobName": "target-job",
    "appId": 10086,
    "buildStatus": "BUILDING",
    "message": "Build is running."
  }
}
```

### 6. 启动任务

`POST /openapi/app/start`

启动任务。可选择从 StreamPark 记录的 latest checkpoint/savepoint 路径恢复。

**请求参数**

| 参数 | 必填 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `jobName` | 是 | string | 无 | Flink 任务名称，必须唯一定位一个任务。 |
| `argument` | 否 | string | 使用任务已保存 `args` | 启动参数，绑定到 `Application.args`。 |
| `restoreFromLatestCheckpoint` | 否 | boolean | `false` | 是否从 latest checkpoint/savepoint 恢复。 |
| `allowNonRestored` | 否 | boolean | `false` | 是否允许跳过无法恢复的 state。 |

**响应字段**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `data` | boolean | 启动提交成功时为 `true`。 |

**业务逻辑**

如果 `restoreFromLatestCheckpoint=true`，服务端读取 latest 路径并设置为启动恢复路径。没有可用 latest 路径时，接口失败并返回 `The application jobName=%s has no available checkpoint, start failed.`。

接口返回只表示启动请求已提交，不代表 Flink 作业已经进入 `RUNNING`。

**示例**

```bash
curl -X POST "${BASE_URL}/openapi/app/start" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${JOB_NAME}" \
  --data-urlencode "restoreFromLatestCheckpoint=true" \
  --data-urlencode "allowNonRestored=false"
```

```json
{
  "status": "success",
  "code": 200,
  "data": true
}
```

### 7. 停止任务

`POST /openapi/app/cancel`

停止任务。可选择在停止前触发 savepoint。

**请求参数**

| 参数 | 必填 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `jobName` | 是 | string | 无 | Flink 任务名称，必须唯一定位一个任务。 |
| `triggerSavepoint` | 否 | boolean | `false` | 停止前是否触发 savepoint。 |
| `savepointPath` | 否 | string | 使用任务或 Flink 默认配置 | savepoint 目录，仅 `triggerSavepoint=true` 时生效。 |
| `drain` | 否 | boolean | `false` | 是否发送 max watermark 后停止。 |

**响应字段**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `status` | string | 成功为 `success`。该接口成功时通常不返回 `data`。 |

**示例**

```bash
curl -X POST "${BASE_URL}/openapi/app/cancel" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${JOB_NAME}" \
  --data-urlencode "triggerSavepoint=true" \
  --data-urlencode "savepointPath=hdfs:///streampark/savepoints" \
  --data-urlencode "drain=false"
```

```json
{
  "status": "success",
  "code": 200
}
```

### 8. 重启任务

`POST /openapi/app/restart`

同步执行停止、等待可启动、读取 latest checkpoint/savepoint、重新启动。调用方应设置足够长的 HTTP 超时时间。

**请求参数**

| 参数 | 必填 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `jobName` | 是 | string | 无 | Flink 任务名称，必须唯一定位一个任务。 |
| `allowNonRestored` | 否 | boolean | `false` | 启动恢复时是否允许跳过无法恢复的 state。 |
| `drain` | 否 | boolean | `false` | 停止阶段是否发送 max watermark 后停止。 |

**响应字段**

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `data` | boolean | 重启提交成功时为 `true`。 |

**业务逻辑**

restart 停止阶段强制不触发 savepoint，也不接受调用方传入恢复路径。停止后每 5 秒检查一次任务是否可启动，最长等待时间默认 60 分钟。任务可启动后，服务端读取 latest checkpoint/savepoint 路径并用于启动恢复。

没有 latest 路径时，接口失败并返回 `The application jobName=%s has no available checkpoint, restart failed.`。

该接口要求调用方同时具备 `app:start` 和 `app:cancel` 权限。

**示例**

```bash
curl -X POST "${BASE_URL}/openapi/app/restart" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${JOB_NAME}" \
  --data-urlencode "allowNonRestored=false" \
  --data-urlencode "drain=false"
```

```json
{
  "status": "success",
  "code": 200,
  "data": true
}
```

## 一键测试脚本示例

```bash
#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:10000}"
TOKEN="${TOKEN:?TOKEN is required}"
SRC_JOB_NAME="${SRC_JOB_NAME:?SRC_JOB_NAME is required}"
JOB_NAME="${JOB_NAME:-automation-sql-demo-$(date +%s)}"

curl -s -X POST "${BASE_URL}/openapi/app/deploy" \
  -H "Authorization: ${TOKEN}" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "srcJobName=${SRC_JOB_NAME}" \
  --data-urlencode "dstJobName=${JOB_NAME}" \
  --data-urlencode "mainClass=org.example.MainJob" \
  --data-urlencode "flinkSql=CREATE TABLE source_table (id INT) WITH ('connector'='datagen'); CREATE TABLE sink_table (id INT) WITH ('connector'='print'); INSERT INTO sink_table SELECT id FROM source_table;" \
  --data-urlencode "forceBuild=false"

echo "JOB_NAME=${JOB_NAME}"

curl -s -X POST "${BASE_URL}/openapi/app/build" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${JOB_NAME}" \
  --data-urlencode "forceBuild=false"
```

## 注意事项

`/openapi/app/get` 返回完整任务详情，`dynamicProperties` 等字段可能包含对象存储或集群访问配置。不要把完整响应直接打印到外部日志或工单。

deploy 已存在目标任务时保持幂等，不会根据本次请求的覆盖字段修改已有任务。如果需要修改已有任务，调用 `/openapi/app/update`。

构建完成不代表任务已启动。构建完成后需要显式调用 `/openapi/app/start`。
