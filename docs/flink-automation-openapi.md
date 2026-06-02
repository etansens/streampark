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

# Flink Task Automation OpenAPI

本文档描述 StreamPark Console 中用于 Flink 任务自动化部署的 REST API 和 curl 测试用例。

## 基础约定

所有 `/openapi/*` 接口使用访问令牌认证：

```bash
export BASE_URL="http://localhost:10000"
export TOKEN="replace-with-streampark-access-token"
export TEAM_ID="1"
export VERSION_ID="1"
export APP_ID="1"
```

通用请求头：

```bash
-H "Authorization: ${TOKEN}"
```

请求体均为 `application/x-www-form-urlencoded`。当前 Controller 使用 Spring MVC 表单参数绑定，不使用 JSON body。

## 枚举

| 参数 | 值 | 含义 |
| --- | --- | --- |
| `jobType` | `1` | Jar/custom code job |
| `jobType` | `2` | Flink SQL job |
| `appType` | `1` | StreamPark Flink |
| `appType` | `2` | Apache Flink |
| `executionMode` | `1` | remote/standalone |
| `executionMode` | `2` | yarn-per-job |
| `executionMode` | `3` | yarn-session |
| `executionMode` | `4` | yarn-application |
| `executionMode` | `5` | kubernetes-session |
| `executionMode` | `6` | kubernetes-application |
| `resourceFrom` | `1` | CICD/Git 构建 |
| `resourceFrom` | `2` | 镜像内 Jar |
| `format` | `1` | YAML |
| `format` | `2` | properties |
| `format` | `3` | HOCON |

## 推荐自动化流程

1. 创建或复制 Flink App，业务 Jar 默认已经包含在运行镜像中。
2. 更新任务配置、SQL、主类、参数或动态属性。
3. 触发构建发布。
4. 轮询构建状态和任务详情。
5. 启动任务，可指定 savepoint/checkpoint 恢复。
6. 停止任务，可先触发 savepoint。
7. 重启任务，可封装为 stop + wait + start。

## OpenAPI 接口

管理接口对应关系：

| 管理操作 | 控制台内部接口 | OpenAPI 接口 |
| --- | --- | --- |
| 获取任务 | `POST /flink/app/get` | `POST /openapi/app/get` |
| 复制任务 | `POST /flink/app/copy` | `POST /openapi/app/copy` |
| 编辑任务 | `POST /flink/app/update` | `POST /openapi/app/update` |
| 编译任务 | `POST /flink/pipe/build` | `POST /openapi/app/build` |
| 启动任务 | `POST /flink/app/start` | `POST /openapi/app/start` |
| 停止任务 | `POST /flink/app/cancel` | `POST /openapi/app/cancel` |

### 查询任务详情

`POST /openapi/app/get`

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `id` | 是 | Flink App ID |

```bash
curl -X POST "${BASE_URL}/openapi/app/get" \
  -H "Authorization: ${TOKEN}" \
  -d "id=${APP_ID}"
```

等价于控制台内部接口：

```bash
curl -XPOST 'http://10.32.2.101:31000/flink/app/get?id=10074&teamId=100000'
```

### 复制任务

`POST /openapi/app/copy`

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `id` | 是 | 源 Flink App ID |
| `jobName` | 是 | 新任务名称，不能与现有任务重复 |
| `teamId` | 是 | Team ID，用于权限范围校验 |
| `argument` | 否 | 复制后任务的运行参数覆盖值，绑定到 `args` |

复制任务会沿用源任务的 SQL、Jar、配置、Flink 版本、执行模式、动态参数、checkpoint/savepoint 相关配置等，返回新任务 ID。

```bash
curl -X POST "${BASE_URL}/openapi/app/copy" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "jobName=sql2iceberg-demo-2" \
  --data-urlencode "teamId=${TEAM_ID}"
```

等价于控制台内部接口：

```bash
curl -XPOST 'http://10.32.2.101:31000/flink/app/copy?id=10074&jobName=sql2iceberg-demo-2&teamId=100000'
```

### 创建 Flink SQL 任务

`POST /openapi/app/create`

常用参数：

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `teamId` | 是 | Team ID |
| `jobName` | 是 | 任务名称 |
| `jobType` | 是 | `2` 表示 Flink SQL |
| `executionMode` | 是 | 执行模式 |
| `versionId` | 是 | Flink 版本 ID |
| `appType` | 是 | SQL 任务使用 `1` |
| `flinkSql` | 是 | SQL 内容 |
| `dependency` | 否 | 依赖 JSON 字符串 |
| `config` | 否 | 配置内容 |
| `format` | 否 | 配置格式 |
| `dynamicProperties` | 否 | Flink `-D` 参数 |
| `args` | 否 | 程序参数 |
| `options` | 否 | 任务选项 JSON 字符串 |
| `resolveOrder` | 否 | classloader resolve order |
| `restartSize` | 否 | 失败自动重启次数 |
| `alertId` | 否 | 告警 ID |
| `cpMaxFailureInterval` | 否 | checkpoint 失败监控窗口 |
| `cpFailureRateInterval` | 否 | checkpoint 失败次数阈值 |
| `cpFailureAction` | 否 | `1` 告警，`2` 重启 |

```bash
curl -X POST "${BASE_URL}/openapi/app/create" \
  -H "Authorization: ${TOKEN}" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "teamId=${TEAM_ID}" \
  --data-urlencode "jobName=demo-sql-job" \
  --data-urlencode "jobType=2" \
  --data-urlencode "executionMode=4" \
  --data-urlencode "versionId=${VERSION_ID}" \
  --data-urlencode "appType=1" \
  --data-urlencode "flinkSql=CREATE TABLE source_table (id INT) WITH ('connector'='datagen'); CREATE TABLE sink_table (id INT) WITH ('connector'='print'); INSERT INTO sink_table SELECT id FROM source_table;" \
  --data-urlencode "dynamicProperties=-Dstate.savepoints.dir=hdfs:///streampark/savepoints -Dexecution.checkpointing.interval=60000" \
  --data-urlencode "options={}" \
  --data-urlencode "resolveOrder=0" \
  --data-urlencode "restartSize=3"
```

### 创建镜像内 Jar 任务

`POST /openapi/app/create`

适用于业务 Jar 已经包含在 Flink 运行镜像或任务运行环境中的场景。

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `teamId` | 是 | Team ID |
| `jobName` | 是 | 任务名称 |
| `jobType` | 是 | `1` 表示 Jar/custom code |
| `executionMode` | 是 | 执行模式 |
| `versionId` | 是 | Flink 版本 ID |
| `appType` | 是 | Apache Flink 使用 `2` |
| `resourceFrom` | 是 | 镜像内 Jar 使用 `2` |
| `jar` | 是 | 镜像内 Jar 名称或路径 |
| `mainClass` | 是 | 主类 |
| `dependency` | 否 | 依赖 JSON 字符串 |
| `dynamicProperties` | 否 | Flink `-D` 参数 |
| `args` | 否 | 程序参数 |

curl -X POST "${BASE_URL}/openapi/app/create" \
  -H "Authorization: ${TOKEN}" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "teamId=${TEAM_ID}" \
  --data-urlencode "jobName=demo-jar-job" \
  --data-urlencode "jobType=1" \
  --data-urlencode "executionMode=4" \
  --data-urlencode "versionId=${VERSION_ID}" \
  --data-urlencode "appType=2" \
  --data-urlencode "resourceFrom=2" \
  --data-urlencode "jar=/opt/flink/usrlib/example-flink-job.jar" \
  --data-urlencode "mainClass=com.example.flink.DemoJob" \
  --data-urlencode "args=--env prod --parallelism 2" \
  --data-urlencode "dynamicProperties=-Dstate.savepoints.dir=hdfs:///streampark/savepoints -Dexecution.checkpointing.interval=60000" \
  --data-urlencode "options={}" \
  --data-urlencode "resolveOrder=0" \
  --data-urlencode "restartSize=3"
```

### 更新任务

`POST /openapi/app/update`

当前更新接口复用控制台 `ApplicationService.update()`，建议传完整任务配置，不建议只传局部字段，避免未传字段被置空。

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `id` | 是 | Flink App ID |
| `jobName` | 建议 | 任务名称 |
| `executionMode` | 建议 | 执行模式 |
| `versionId` | 建议 | Flink 版本 ID |
| `flinkSql` | SQL 任务 | SQL 内容 |
| `sqlId` | SQL 任务建议 | 当前 SQL 版本 ID |
| `mainClass` | Jar 任务 | 主类 |
| `args` | 否 | Program Args |
| `dependency` | 否 | 依赖 JSON 字符串 |
| `config` | 否 | 配置内容 |
| `format` | 否 | 配置格式 |
| `dynamicProperties` | 否 | Flink `-D` 参数 |
| `k8sPodTemplate` | 否 | Kubernetes 通用 Pod Template YAML |
| `k8sJmPodTemplate` | 否 | Kubernetes JobManager Pod Template YAML |
| `k8sTmPodTemplate` | 否 | Kubernetes TaskManager Pod Template YAML |

```bash
curl -X POST "${BASE_URL}/openapi/app/update" \
  -H "Authorization: ${TOKEN}" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "teamId=${TEAM_ID}" \
  --data-urlencode "jobName=demo-sql-job" \
  --data-urlencode "executionMode=4" \
  --data-urlencode "versionId=${VERSION_ID}" \
  --data-urlencode "flinkSql=CREATE TABLE source_table (id INT) WITH ('connector'='datagen'); CREATE TABLE sink_table (id INT) WITH ('connector'='print'); INSERT INTO sink_table SELECT id FROM source_table;" \
  --data-urlencode "args=--env test --parallelism 2" \
  --data-urlencode "dynamicProperties=-Dstate.savepoints.dir=hdfs:///streampark/savepoints -Dexecution.checkpointing.interval=30000" \
  --data-urlencode "k8sPodTemplate=${POD_TEMPLATE_YAML}" \
  --data-urlencode "k8sJmPodTemplate=${JM_POD_TEMPLATE_YAML}" \
  --data-urlencode "k8sTmPodTemplate=${TM_POD_TEMPLATE_YAML}" \
  --data-urlencode "options={}" \
  --data-urlencode "resolveOrder=0" \
  --data-urlencode "restartSize=3"
```

等价于控制台内部接口：

```bash
curl -XPOST 'http://10.32.2.101:31000/flink/app/update'
```

### 构建/发布任务

`POST /openapi/app/build`

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `id` | 是 | Flink App ID |
| `forceBuild` | 否 | 是否强制构建，默认 `false` |

```bash
curl -X POST "${BASE_URL}/openapi/app/build" \
  -H "Authorization: ${TOKEN}" \
  -d "id=${APP_ID}" \
  -d "forceBuild=false"
```

等价于控制台内部接口：

```bash
curl -XPOST 'http://10.32.2.101:31000/flink/pipe/build?appId=10079&forceBuild=false&teamId=100000'
```

### 查询构建状态

`POST /openapi/app/build/status`

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `id` | 是 | Flink App ID |

```bash
curl -X POST "${BASE_URL}/openapi/app/build/status" \
  -H "Authorization: ${TOKEN}" \
  -d "id=${APP_ID}"
```

### 启动任务

`POST /openapi/app/start`

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `id` | 是 | Flink App ID |
| `argument` | 否 | 启动参数，绑定到 `args` |
| `restoreFromSavepoint` | 否 | 是否从 savepoint/checkpoint 恢复 |
| `savepointPath` | 否 | 指定 savepoint/checkpoint 路径；为空时使用最新记录 |
| `allowNonRestored` | 否 | 是否允许跳过无法恢复的 state |

普通启动：

```bash
curl -X POST "${BASE_URL}/openapi/app/start" \
  -H "Authorization: ${TOKEN}" \
  -d "id=${APP_ID}"
```

从指定 savepoint/checkpoint 启动：

```bash
curl -X POST "${BASE_URL}/openapi/app/start" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "restoreFromSavepoint=true" \
  --data-urlencode "savepointPath=hdfs:///streampark/savepoints/demo/savepoint-xxxx" \
  --data-urlencode "allowNonRestored=false"
```

等价于控制台内部接口：

```bash
curl -XPOST 'http://10.32.2.101:31000/flink/app/check_start?id=10074&teamId=100000'
curl -XPOST 'http://10.32.2.101:31000/flink/app/start?id=10074&restoreOrTriggerSavepoint=true&savepointPath=s3%3A%2F%2Feq-bigdata-cloud-test%2Fflink%2Fcheckpoint%2F481d9457627e70297ac587e41ee58041%2Fchk-10082&allowNonRestored=false&teamId=100000'
```

### 停止任务

`POST /openapi/app/cancel`

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `id` | 是 | Flink App ID |
| `triggerSavepoint` | 否 | 停止前是否触发 savepoint |
| `savepointPath` | 否 | savepoint 目录 |
| `drain` | 否 | 是否发送 max watermark 后停止 |

普通停止：

```bash
curl -X POST "${BASE_URL}/openapi/app/cancel" \
  -H "Authorization: ${TOKEN}" \
  -d "id=${APP_ID}"
```

停止前触发 savepoint：

```bash
curl -X POST "${BASE_URL}/openapi/app/cancel" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "triggerSavepoint=true" \
  --data-urlencode "savepointPath=hdfs:///streampark/savepoints" \
  --data-urlencode "drain=false"
```

等价于控制台内部接口：

```bash
curl -XPOST 'http://10.32.2.101:31000/flink/app/cancel?id=10074&restoreOrTriggerSavepoint=false&teamId=100000'
```

### 重启任务

`POST /openapi/app/restart`

该接口同步执行：先停止任务，等待任务进入可启动状态，再启动任务。调用方应设置足够的 HTTP 超时时间。

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `id` | 是 | Flink App ID |
| `triggerSavepoint` | 否 | 重启停止前是否触发 savepoint |
| `restoreFromSavepoint` | 否 | 重启启动时是否从 savepoint/checkpoint 恢复 |
| `savepointPath` | 否 | savepoint/checkpoint 路径 |
| `allowNonRestored` | 否 | 是否允许跳过无法恢复的 state |
| `drain` | 否 | 停止前是否 drain |

```bash
curl -X POST "${BASE_URL}/openapi/app/restart" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "triggerSavepoint=true" \
  --data-urlencode "restoreFromSavepoint=true" \
  --data-urlencode "savepointPath=hdfs:///streampark/savepoints" \
  --data-urlencode "allowNonRestored=false" \
  --data-urlencode "drain=false"
```

### 触发 Savepoint

`POST /openapi/app/savepoint/trigger`

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `id` | 是 | Flink App ID |
| `savepointPath` | 否 | savepoint 目录；为空时按任务配置推断 |

```bash
curl -X POST "${BASE_URL}/openapi/app/savepoint/trigger" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "savepointPath=hdfs:///streampark/savepoints"
```

### 查询最新 Savepoint/Checkpoint

`POST /openapi/app/savepoint/latest`

| 参数 | 必填 | 说明 |
| --- | --- | --- |
| `id` | 是 | Flink App ID |

```bash
curl -X POST "${BASE_URL}/openapi/app/savepoint/latest" \
  -H "Authorization: ${TOKEN}" \
  -d "id=${APP_ID}"
```

## 控制台内部辅助接口

以下接口已有或已补齐，但不是全部都带 `@OpenAPI` 注解。使用 API Token 调用时，如果不是 `/openapi/*` 接口，可能会被 OpenAPI 拦截器拒绝；控制台会话或白名单场景可使用。

### SQL 校验

`POST /flink/sql/verify`

```bash
curl -X POST "${BASE_URL}/flink/sql/verify" \
  -H "Cookie: SESSION=replace-with-console-session" \
  --data-urlencode "teamId=${TEAM_ID}" \
  --data-urlencode "versionId=${VERSION_ID}" \
  --data-urlencode "sql=CREATE TABLE source_table (id INT) WITH ('connector'='datagen');"
```

### Savepoint/Checkpoint 历史

`POST /flink/savepoint/history`

```bash
curl -X POST "${BASE_URL}/flink/savepoint/history" \
  -H "Cookie: SESSION=replace-with-console-session" \
  -d "appId=${APP_ID}" \
  -d "teamId=${TEAM_ID}" \
  -d "pageNum=1" \
  -d "pageSize=10"
```

### 控制台 latest Savepoint/Checkpoint

`POST /flink/savepoint/latest`

```bash
curl -X POST "${BASE_URL}/flink/savepoint/latest" \
  -H "Cookie: SESSION=replace-with-console-session" \
  -d "appId=${APP_ID}" \
  -d "teamId=${TEAM_ID}"
```

## 一键测试脚本示例

```bash
#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:10000}"
TOKEN="${TOKEN:?TOKEN is required}"
TEAM_ID="${TEAM_ID:-1}"
VERSION_ID="${VERSION_ID:-1}"

APP_ID=$(curl -s -X POST "${BASE_URL}/openapi/app/create" \
  -H "Authorization: ${TOKEN}" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "teamId=${TEAM_ID}" \
  --data-urlencode "jobName=automation-sql-demo-$(date +%s)" \
  --data-urlencode "jobType=2" \
  --data-urlencode "executionMode=4" \
  --data-urlencode "versionId=${VERSION_ID}" \
  --data-urlencode "appType=1" \
  --data-urlencode "flinkSql=CREATE TABLE source_table (id INT) WITH ('connector'='datagen'); CREATE TABLE sink_table (id INT) WITH ('connector'='print'); INSERT INTO sink_table SELECT id FROM source_table;" \
  --data-urlencode "options={}" \
  --data-urlencode "resolveOrder=0" | sed -n 's/.*"id":\([0-9]*\).*/\1/p')

echo "APP_ID=${APP_ID}"

curl -s -X POST "${BASE_URL}/openapi/app/build" \
  -H "Authorization: ${TOKEN}" \
  -d "id=${APP_ID}" \
  -d "forceBuild=false"

curl -s -X POST "${BASE_URL}/openapi/app/build/status" \
  -H "Authorization: ${TOKEN}" \
  -d "id=${APP_ID}"
```

## 注意事项

创建和更新接口当前复用 StreamPark Console 原有 `Application` 参数绑定，参数是表单字段，不是 JSON。

更新接口建议传完整配置。只传局部字段可能导致原有字段被置空。

`build` 成功表示构建流程已提交或不需要构建；实际部署进度建议轮询 `build/status` 和 `app/get`。

`savepointPath` 可以是 savepoint 或 checkpoint 路径。启动时 `restoreFromSavepoint=true` 且不传 `savepointPath`，系统会尝试读取最新记录。

如果直接调用 `/flink/*` 内部接口，需要使用控制台登录态或配置 OpenAPI 白名单。
