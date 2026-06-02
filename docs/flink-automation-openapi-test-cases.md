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

# Flink Automation OpenAPI Test Cases

本文档用于验证 Flink 任务自动化部署 OpenAPI。测试前提是业务 Jar 已包含在 Flink 运行镜像或任务运行环境中，不测试 Jar 上传接口。

## 测试环境变量

```bash
export BASE_URL="http://10.32.2.101:31000"
export TOKEN="replace-with-openapi-token"
export START_ONLY_TOKEN="replace-with-token-that-only-has-app-start-permission"
export TEAM_ID="100000"
export SOURCE_APP_ID="10074"
export APP_ID="10074"
export COPY_JOB_NAME="sql2iceberg-demo-openapi-copy-$(date +%Y%m%d%H%M%S)"
export VERSION_ID="1"
export IMAGE_JAR="/opt/flink/usrlib/example-flink-job.jar"
export MAIN_CLASS="com.example.flink.DemoJob"
export CHECKPOINT_PATH="replace-with-latest-checkpoint-or-savepoint-path"
export POD_TEMPLATE_YAML="replace-with-urlencoded-or-single-line-pod-template-yaml"
export JM_POD_TEMPLATE_YAML="replace-with-urlencoded-or-single-line-jm-pod-template-yaml"
export TM_POD_TEMPLATE_YAML="replace-with-urlencoded-or-single-line-tm-pod-template-yaml"
```

通用认证头：

```bash
-H "Authorization: ${TOKEN}"
```

## 正向测试用例

| 用例 ID | 接口 | 场景 | 预期结果 |
| --- | --- | --- | --- |
| TC-001 | `POST /openapi/app/get` | 获取任务详情 | 返回 `status=success`，`data.id` 等于请求 ID |
| TC-002 | `POST /openapi/app/copy` | 复制任务 | 返回 `status=success`，`data.id` 为新任务 ID |
| TC-003 | `POST /openapi/app/update` | 更新 SQL 任务配置 | 返回 `status=success`，任务进入待发布状态 |
| TC-004 | `POST /openapi/app/update` | 更新 Program Args 和 Kubernetes Pod Template | 返回 `status=success`，字段保存成功 |
| TC-005 | `POST /openapi/app/build` | 编译/发布任务 | 返回构建提交成功或无需构建 |
| TC-006 | `POST /openapi/app/build/status` | 查询构建状态 | 返回当前构建流水线状态或 `data=null` |
| TC-007 | `POST /openapi/app/start` | 普通启动任务 | 返回 `status=success`，轮询进入 `RUNNING` |
| TC-008 | `POST /openapi/app/start` | 从指定 checkpoint/savepoint 启动 | 返回 `status=success`，任务使用指定恢复路径 |
| TC-009 | `POST /openapi/app/start` | 从最新 checkpoint/savepoint 启动 | 不传 `savepointPath` 时读取 latest 记录并进入 `RUNNING` |
| TC-010 | `POST /openapi/app/cancel` | 普通停止任务 | 返回 `status=success`，轮询进入 `CANCELED` |
| TC-011 | `POST /openapi/app/cancel` | 停止前触发 savepoint | 返回 `status=success`，生成 latest savepoint 记录 |
| TC-012 | `POST /openapi/app/restart` | 重启任务 | 返回 `status=success`，任务先停止再启动 |
| TC-013 | `POST /openapi/app/savepoint/trigger` | 手动触发 savepoint | 返回 `status=success` |
| TC-014 | `POST /openapi/app/savepoint/latest` | 查询最新 savepoint/checkpoint | 返回最新记录或 `data=null` |
| TC-015 | 多个接口 | 全流程回归：复制、更新、构建、启动、停止并触发 savepoint | 任务完整经历 `copy -> update -> build -> RUNNING -> CANCELED`，latest 为新 savepoint |

### TC-001 获取任务详情

```bash
curl -X POST "${BASE_URL}/openapi/app/get" \
  -H "Authorization: ${TOKEN}" \
  -d "id=${APP_ID}"
```

检查点：响应中 `status=success`，`data.id=${APP_ID}`。注意 `flinkSql` 在 `get` 响应中可能是 Base64 编码，`dynamicProperties` 可能包含敏感配置，不应完整打印到日志。

### TC-002 复制任务

```bash
curl -X POST "${BASE_URL}/openapi/app/copy" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "id=${SOURCE_APP_ID}" \
  --data-urlencode "jobName=${COPY_JOB_NAME}" \
  --data-urlencode "teamId=${TEAM_ID}"
```

检查点：响应中 `status=success`，`data.id` 为新任务 ID。后续可将新 ID 设置为 `APP_ID`。

### TC-003 更新 SQL 任务配置

```bash
curl -X POST "${BASE_URL}/openapi/app/update" \
  -H "Authorization: ${TOKEN}" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "teamId=${TEAM_ID}" \
  --data-urlencode "jobName=${COPY_JOB_NAME}" \
  --data-urlencode "executionMode=4" \
  --data-urlencode "versionId=${VERSION_ID}" \
  --data-urlencode "flinkSql=CREATE TABLE source_table (id INT) WITH ('connector'='datagen'); CREATE TABLE sink_table (id INT) WITH ('connector'='print'); INSERT INTO sink_table SELECT id FROM source_table;" \
  --data-urlencode "args=--env test --parallelism 2" \
  --data-urlencode "dynamicProperties=-Dstate.savepoints.dir=s3://eq-bigdata-cloud-test/flink/savepoints -Dexecution.checkpointing.interval=60000" \
  --data-urlencode "k8sPodTemplate=${POD_TEMPLATE_YAML}" \
  --data-urlencode "k8sJmPodTemplate=${JM_POD_TEMPLATE_YAML}" \
  --data-urlencode "k8sTmPodTemplate=${TM_POD_TEMPLATE_YAML}" \
  --data-urlencode "options={}" \
  --data-urlencode "resolveOrder=0" \
  --data-urlencode "restartSize=3"
```

检查点：响应中 `status=success`。更新接口建议传完整任务配置，避免未传字段被置空。再次调用 TC-001 时，应能看到 SQL 变更已保存；如需比对 SQL 内容，先按接口返回格式处理 Base64 编码。

### TC-004 更新 Program Args 和 Kubernetes Pod Template

```bash
curl -X POST "${BASE_URL}/openapi/app/update" \
  -H "Authorization: ${TOKEN}" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "teamId=${TEAM_ID}" \
  --data-urlencode "jobName=${COPY_JOB_NAME}" \
  --data-urlencode "executionMode=4" \
  --data-urlencode "versionId=${VERSION_ID}" \
  --data-urlencode "args=--env test --parallelism 2" \
  --data-urlencode "dynamicProperties=-Dstate.savepoints.dir=s3://eq-bigdata-cloud-test/flink/savepoints -Dexecution.checkpointing.interval=60000" \
  --data-urlencode "k8sPodTemplate=${POD_TEMPLATE_YAML}" \
  --data-urlencode "k8sJmPodTemplate=${JM_POD_TEMPLATE_YAML}" \
  --data-urlencode "k8sTmPodTemplate=${TM_POD_TEMPLATE_YAML}" \
  --data-urlencode "options={}" \
  --data-urlencode "resolveOrder=0" \
  --data-urlencode "restartSize=3"
```

检查点：响应中 `status=success`，再次 `get` 可看到 `args` 和三个 `k8s*PodTemplate` 字段已更新。该用例用于防止 `args`、`k8sPodTemplate`、`k8sJmPodTemplate`、`k8sTmPodTemplate` 从 OpenAPI schema 或绑定逻辑中回退。

### TC-005 编译/发布任务

```bash
curl -X POST "${BASE_URL}/openapi/app/build" \
  -H "Authorization: ${TOKEN}" \
  -d "id=${APP_ID}" \
  -d "forceBuild=false"
```

检查点：响应中 `status=success`。如果任务无需构建，响应仍应明确表示可继续后续流程。OpenAPI 参数名必须使用 `id`，不是内部 `/flink/pipe/build` 的 `appId`。

### TC-006 查询构建状态

```bash
curl -X POST "${BASE_URL}/openapi/app/build/status" \
  -H "Authorization: ${TOKEN}" \
  -d "id=${APP_ID}"
```

检查点：构建中返回流水线信息，未构建或无当前流水线时允许 `data=null`。

### TC-007 普通启动任务

```bash
curl -X POST "${BASE_URL}/openapi/app/start" \
  -H "Authorization: ${TOKEN}" \
  -d "id=${APP_ID}"
```

检查点：响应中 `status=success`。启动后轮询 TC-001，任务应从 `STARTING` 进入 `RUNNING`。

### TC-008 从指定 checkpoint/savepoint 启动任务

```bash
curl -X POST "${BASE_URL}/openapi/app/start" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "restoreFromSavepoint=true" \
  --data-urlencode "savepointPath=${CHECKPOINT_PATH}" \
  --data-urlencode "allowNonRestored=false"
```

检查点：响应中 `status=success`。路径中的 `s3://` 由 `--data-urlencode` 自动编码。

### TC-009 从最新 checkpoint/savepoint 启动任务

```bash
curl -X POST "${BASE_URL}/openapi/app/start" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "restoreFromSavepoint=true" \
  --data-urlencode "allowNonRestored=false"
```

检查点：响应中 `status=success`。不传 `savepointPath` 时，服务端应使用 TC-014 返回的 latest checkpoint/savepoint。启动后轮询 TC-001，任务应进入 `RUNNING`，并产生新的 `jobId`。

### TC-010 普通停止任务

```bash
curl -X POST "${BASE_URL}/openapi/app/cancel" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "triggerSavepoint=false"
```

检查点：响应中 `status=success`。停止后轮询 TC-001，任务应从 `CANCELLING` 进入 `CANCELED`。

### TC-011 停止前触发 savepoint

```bash
curl -X POST "${BASE_URL}/openapi/app/cancel" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "triggerSavepoint=true" \
  --data-urlencode "savepointPath=s3://eq-bigdata-cloud-test/flink/savepoints" \
  --data-urlencode "drain=false"
```

检查点：响应中 `status=success`。停止完成后调用 TC-014 查询最新 savepoint/checkpoint，latest 记录应变为 `type=2` 的 savepoint。

### TC-012 重启任务

```bash
curl --max-time 3900 -X POST "${BASE_URL}/openapi/app/restart" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "triggerSavepoint=false" \
  --data-urlencode "restoreFromSavepoint=false" \
  --data-urlencode "allowNonRestored=false" \
  --data-urlencode "drain=false"
```

检查点：响应中 `status=success`。该接口同步等待任务可启动后再启动，客户端超时时间应大于服务端等待停止超时时间。

### TC-013 手动触发 savepoint

```bash
curl -X POST "${BASE_URL}/openapi/app/savepoint/trigger" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "savepointPath=s3://eq-bigdata-cloud-test/flink/savepoints"
```

检查点：响应中 `status=success`。该接口要求任务处于可触发 savepoint 的运行状态。

### TC-014 查询最新 savepoint/checkpoint

```bash
curl -X POST "${BASE_URL}/openapi/app/savepoint/latest" \
  -H "Authorization: ${TOKEN}" \
  -d "id=${APP_ID}"
```

检查点：有历史记录时返回最新 `Savepoint`；没有历史记录时允许 `data=null`。

### TC-015 全流程回归

该用例覆盖已验证的自动化部署主链路，建议作为版本发布前的最小回归：复制源任务，更新 SQL 或参数，构建，启动，等待 `RUNNING`，停止并触发 savepoint，最后确认 latest 记录变成新的 savepoint。

```bash
node <<'NODE'
const baseUrl = process.env.BASE_URL;
const token = process.env.TOKEN;
const headers = { Authorization: token };
const names = { 3: 'STARTING', 5: 'RUNNING', 8: 'CANCELLING', 9: 'CANCELED' };

async function post(path, params) {
  const res = await fetch(`${baseUrl}${path}`, {
    method: 'POST',
    headers,
    body: new URLSearchParams(params),
  });
  const json = await res.json();
  console.log(path, res.status, json.status, json.code, json.message || '', json.data || '');
  if (res.status !== 200 || json.status !== 'success') {
    throw new Error(`${path} failed`);
  }
  return json.data;
}

async function waitForState(id, expected) {
  for (let i = 1; i <= 30; i++) {
    const app = await post('/openapi/app/get', { id });
    console.log(`poll=${i} state=${app.state} ${names[app.state] || ''} jobId=${app.jobId || ''}`);
    if (app.state === expected) return app;
    await new Promise((resolve) => setTimeout(resolve, 10000));
  }
  throw new Error(`app ${id} did not reach state ${expected}`);
}

const copied = await post('/openapi/app/copy', {
  id: process.env.SOURCE_APP_ID,
  jobName: process.env.COPY_JOB_NAME,
  teamId: process.env.TEAM_ID,
});
const appId = copied.id;

await post('/openapi/app/update', {
  id: appId,
  teamId: process.env.TEAM_ID,
  jobName: process.env.COPY_JOB_NAME,
  executionMode: '4',
  versionId: process.env.VERSION_ID,
  flinkSql: "CREATE TABLE source_table (id INT) WITH ('connector'='datagen'); CREATE TABLE sink_table (id INT) WITH ('connector'='print'); INSERT INTO sink_table SELECT id FROM source_table;",
  options: '{}',
  resolveOrder: '0',
  restartSize: '3',
});

await post('/openapi/app/build', { id: appId, forceBuild: 'true' });
await post('/openapi/app/build/status', { id: appId });
await post('/openapi/app/start', { id: appId });
await waitForState(appId, 5);
await post('/openapi/app/cancel', { id: appId, triggerSavepoint: 'true' });
await waitForState(appId, 9);
const latest = await post('/openapi/app/savepoint/latest', { id: appId });
if (!latest || latest.type !== 2 || !latest.path) {
  throw new Error('latest savepoint was not created by cancel');
}
NODE
```

检查点：脚本无异常退出；复制任务进入 `RUNNING` 后被停止到 `CANCELED`；latest 记录 `type=2` 且 `path` 为新 savepoint 路径。

## 异常测试用例

| 用例 ID | 接口 | 场景 | 预期结果 |
| --- | --- | --- | --- |
| TC-N001 | 任意 OpenAPI | 不传 `Authorization` | 请求被拒绝或返回认证失败 |
| TC-N002 | 任意 OpenAPI | 使用无效或旧 token | 请求被拒绝或返回认证失败 |
| TC-N003 | `POST /openapi/app/get` | `id` 不存在 | 返回失败信息或 `data=null`，不得服务端异常崩溃 |
| TC-N004 | `POST /openapi/app/copy` | `jobName` 重复 | 返回失败信息，提示任务名不能重复 |
| TC-N005 | `POST /openapi/app/copy` | 不传 `teamId` | 权限范围校验失败或参数校验失败 |
| TC-N006 | `POST /openapi/app/build` | 使用 `appId` 而不是 `id` | 参数校验失败，不应静默构建错误任务 |
| TC-N007 | `POST /openapi/app/start` | checkpoint 路径不存在 | 返回启动失败信息，任务不应进入错误的运行态 |
| TC-N008 | `POST /openapi/app/restart` | token 只有启动权限没有停止权限 | 请求被拒绝，不能停止任务 |
| TC-N009 | `POST /flink/savepoint/history` | 用 OpenAPI token 调内部非白名单接口 | 返回 OpenAPI unsupported，不应暴露内部接口 |

### TC-N001 缺少认证头

```bash
curl -X POST "${BASE_URL}/openapi/app/get" \
  -d "id=${APP_ID}"
```

检查点：请求不能成功读取任务详情。

### TC-N002 无效或旧 token

```bash
curl -X POST "${BASE_URL}/openapi/app/get" \
  -H "Authorization: invalid-token" \
  -d "id=${APP_ID}"
```

检查点：请求不能成功读取任务详情。该用例覆盖 token 解密和比对逻辑，避免旧的随机 IV 密文比较问题回归。

### TC-N003 查询不存在任务

```bash
curl -X POST "${BASE_URL}/openapi/app/get" \
  -H "Authorization: ${TOKEN}" \
  -d "id=999999999"
```

检查点：返回可解析的错误响应或 `data=null`，服务端日志不应出现未处理异常。

### TC-N004 复制任务名重复

```bash
curl -X POST "${BASE_URL}/openapi/app/copy" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "id=${SOURCE_APP_ID}" \
  --data-urlencode "jobName=${COPY_JOB_NAME}" \
  --data-urlencode "teamId=${TEAM_ID}"
```

检查点：第一次复制成功后，再次用相同 `jobName` 调用应失败并提示任务名不能重复。

### TC-N005 复制任务缺少 teamId

```bash
curl -X POST "${BASE_URL}/openapi/app/copy" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "id=${SOURCE_APP_ID}" \
  --data-urlencode "jobName=${COPY_JOB_NAME}-missing-team"
```

检查点：请求不能绕过 Team 权限范围校验。

### TC-N006 构建接口误用 appId 参数

```bash
curl -X POST "${BASE_URL}/openapi/app/build" \
  -H "Authorization: ${TOKEN}" \
  -d "appId=${APP_ID}" \
  -d "forceBuild=true"
```

检查点：请求应返回参数校验失败，例如 `id must not be null`。OpenAPI 文档和调用方必须使用 `id`。

### TC-N007 使用不存在 checkpoint 启动

```bash
curl -X POST "${BASE_URL}/openapi/app/start" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "restoreFromSavepoint=true" \
  --data-urlencode "savepointPath=s3://eq-bigdata-cloud-test/flink/checkpoint/not-exists/chk-00000" \
  --data-urlencode "allowNonRestored=false"
```

检查点：请求返回启动失败信息，不应静默成功。

### TC-N008 重启权限不足

```bash
curl --max-time 3900 -X POST "${BASE_URL}/openapi/app/restart" \
  -H "Authorization: ${START_ONLY_TOKEN}" \
  --data-urlencode "id=${APP_ID}" \
  --data-urlencode "triggerSavepoint=false" \
  --data-urlencode "restoreFromSavepoint=false"
```

检查点：仅有 `app:start` 权限、没有 `app:cancel` 权限的 token 不能调用重启接口。

### TC-N009 OpenAPI token 调内部 savepoint history

```bash
curl -X POST "${BASE_URL}/flink/savepoint/history" \
  -H "Authorization: ${TOKEN}" \
  -d "appId=${APP_ID}" \
  -d "teamId=${TEAM_ID}" \
  -d "pageNum=1" \
  -d "pageSize=10"
```

检查点：请求应失败并提示 `Openapi unsupported: /flink/savepoint/history`。如果需要对外暴露完整列表，应新增 `/openapi/app/savepoint/history`，不能依赖内部接口。
