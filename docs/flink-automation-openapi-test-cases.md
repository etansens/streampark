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
export INTERNAL_TEAM_ID="100000"
export INTERNAL_APP_ID="10074"
export SRC_JOB_NAME="sql2iceberg-demo"
export JOB_NAME="sql2iceberg-demo"
export COPY_JOB_NAME="sql2iceberg-demo-openapi-copy-$(date +%Y%m%d%H%M%S)"
export DEPLOY_JOB_NAME="sql2iceberg-demo-openapi-deploy-$(date +%Y%m%d%H%M%S)"
export VERSION_ID="1"
export IMAGE_JAR="/opt/flink/usrlib/example-flink-job.jar"
export MAIN_CLASS="com.example.flink.DemoJob"
export POD_TEMPLATE_YAML="replace-with-urlencoded-or-single-line-pod-template-yaml"
```

通用认证头：

```bash
-H "Authorization: ${TOKEN}"
```

## 正向测试用例

| 用例 ID | 接口 | 场景 | 预期结果 |
| --- | --- | --- | --- |
| TC-001 | `POST /openapi/app/get` | 获取任务详情 | 返回 `status=success`，`data.jobName` 等于请求任务名 |
| TC-002 | `POST /openapi/app/copy` | 复制任务 | 返回 `status=success`，`data.id` 为新任务 ID |
| TC-003 | `POST /openapi/app/deploy` | 一键部署 | 不存在目标任务时复制、更新并构建；已存在时直接返回状态 |
| TC-004 | `POST /openapi/app/update` | 更新 SQL 任务配置 | 返回 `status=success`，任务进入待发布状态 |
| TC-005 | `POST /openapi/app/update` | 更新 Program Args 和 Kubernetes Pod Template | 返回 `status=success`，字段保存成功 |
| TC-006 | `POST /openapi/app/build` | 编译/发布任务并查询构建状态 | 返回 `BUILDING`、`COMPLETED` 或 `FAILED` |
| TC-007 | `POST /openapi/app/build` | 重复调用查询构建状态 | 构建中不重复提交；显式 `forceBuild=true` 才重新构建 |
| TC-008 | `POST /openapi/app/start` | 普通启动任务 | 返回 `status=success`，轮询进入 `RUNNING` |
| TC-009 | `POST /openapi/app/start` | 从最新 checkpoint 启动 | 服务端读取 latest checkpoint 记录并进入 `RUNNING` |
| TC-010 | `POST /openapi/app/cancel` | 普通停止任务 | 返回 `status=success`，轮询进入 `CANCELED` |
| TC-011 | `POST /openapi/app/cancel` | 停止前触发 savepoint | 返回 `status=success`，生成 latest savepoint 记录 |
| TC-012 | `POST /openapi/app/restart` | 重启任务 | 返回 `status=success`，任务先停止再启动 |

### TC-001 获取任务详情

```bash
curl -X POST "${BASE_URL}/openapi/app/get" \
  -H "Authorization: ${TOKEN}" \
  -d "jobName=${JOB_NAME}"
```

检查点：响应中 `status=success`，`data.jobName=${JOB_NAME}`。注意 `flinkSql` 在 `get` 响应中可能是 Base64 编码，`dynamicProperties` 可能包含敏感配置，不应完整打印到日志。

### TC-002 复制任务

```bash
curl -X POST "${BASE_URL}/openapi/app/copy" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "srcJobName=${SRC_JOB_NAME}" \
  --data-urlencode "dstJobName=${COPY_JOB_NAME}"
```

检查点：响应中 `status=success`，`data.id` 为新任务 ID；新任务 `teamId` 与源任务一致。后续任务级 OpenAPI 调用继续使用 `COPY_JOB_NAME`。

### TC-003 一键部署

```bash
curl -X POST "${BASE_URL}/openapi/app/deploy" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "srcJobName=${SRC_JOB_NAME}" \
  --data-urlencode "dstJobName=${DEPLOY_JOB_NAME}" \
  --data-urlencode "mainClass=org.example.MainJob" \
  --data-urlencode "flinkImage=registry.example.com/flink/job-runtime:20260603" \
  --data-urlencode "args=--env test --parallelism 2" \
  --data-urlencode "forceBuild=false"
```

检查点：首次提交时返回 `buildSubmitted=true`；重复提交相同 `DEPLOY_JOB_NAME` 时返回 `buildSubmitted=false`，且不重复复制或构建。

### TC-004 更新 SQL 任务配置

```bash
curl -X POST "${BASE_URL}/openapi/app/update" \
  -H "Authorization: ${TOKEN}" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "jobName=${COPY_JOB_NAME}" \
  --data-urlencode "mainClass=org.example.MainJob" \
  --data-urlencode "flinkSql=CREATE TABLE source_table (id INT) WITH ('connector'='datagen'); CREATE TABLE sink_table (id INT) WITH ('connector'='print'); INSERT INTO sink_table SELECT id FROM source_table;" \
  --data-urlencode "args=--env test --parallelism 2" \
  --data-urlencode "dynamicProperties=-Dstate.savepoints.dir=s3://eq-bigdata-cloud-test/flink/savepoints -Dexecution.checkpointing.interval=60000" \
  --data-urlencode "flinkImage=registry.example.com/flink/job-runtime:20260603" \
  --data-urlencode "k8sPodTemplate=${POD_TEMPLATE_YAML}"
```

检查点：响应中 `status=success`。更新接口支持 `mainClass`、`flinkSql`、`args`、`dynamicProperties`、`flinkImage`、`k8sPodTemplate` 这组局部覆盖字段，未传字段应保持原值；如果显式传入空字符串，则该字段应被清空。再次调用 TC-001 时，应能看到 SQL、`mainClass` 和 `flinkImage` 变更已保存；如需比对 SQL 内容，先按接口返回格式处理 Base64 编码。

### TC-005 更新 Program Args 和 Kubernetes Pod Template

```bash
curl -X POST "${BASE_URL}/openapi/app/update" \
  -H "Authorization: ${TOKEN}" \
  -H "Content-Type: application/x-www-form-urlencoded" \
  --data-urlencode "jobName=${COPY_JOB_NAME}" \
  --data-urlencode "args=--env test --parallelism 2" \
  --data-urlencode "dynamicProperties=-Dstate.savepoints.dir=s3://eq-bigdata-cloud-test/flink/savepoints -Dexecution.checkpointing.interval=60000" \
  --data-urlencode "k8sPodTemplate=${POD_TEMPLATE_YAML}"
```

检查点：响应中 `status=success`，再次 `get` 可看到 `args` 和 `k8sPodTemplate` 字段已更新。该用例用于防止 `args`、`k8sPodTemplate` 从 OpenAPI schema 或绑定逻辑中回退。

### TC-006 编译/发布任务

```bash
curl -X POST "${BASE_URL}/openapi/app/build" \
  -H "Authorization: ${TOKEN}" \
  -d "jobName=${COPY_JOB_NAME}" \
  -d "forceBuild=false"
```

检查点：响应中 `status=success`，`data.buildStatus` 为 `BUILDING`、`COMPLETED` 或 `FAILED`。如果任务无需构建，响应仍应明确表示可继续后续流程。OpenAPI 参数名必须使用 `jobName`，不是内部 `/flink/pipe/build` 的 `appId`。

### TC-007 重复调用查询构建状态

```bash
curl -X POST "${BASE_URL}/openapi/app/build" \
  -H "Authorization: ${TOKEN}" \
  -d "jobName=${COPY_JOB_NAME}" \
  -d "forceBuild=false"
```

检查点：构建中返回 `data.buildStatus=BUILDING`，构建完成返回 `COMPLETED`，构建失败返回 `FAILED`。轮询请求使用 `forceBuild=false`，不应重复提交构建；如果需要重新构建，必须显式传 `forceBuild=true`。

### TC-008 普通启动任务

```bash
curl -X POST "${BASE_URL}/openapi/app/start" \
  -H "Authorization: ${TOKEN}" \
  -d "jobName=${COPY_JOB_NAME}"
```

检查点：响应中 `status=success`。启动后轮询 TC-001，任务应从 `STARTING` 进入 `RUNNING`。

### TC-009 从最新 checkpoint 启动任务

```bash
curl -X POST "${BASE_URL}/openapi/app/start" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${COPY_JOB_NAME}" \
  --data-urlencode "restoreFromLatestCheckpoint=true" \
  --data-urlencode "allowNonRestored=false"
```

检查点：响应中 `status=success`。服务端应内部读取 latest checkpoint 记录；无可用 checkpoint 时请求应失败。

### TC-010 普通停止任务

```bash
curl -X POST "${BASE_URL}/openapi/app/cancel" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${COPY_JOB_NAME}" \
  --data-urlencode "triggerSavepoint=false"
```

检查点：响应中 `status=success`。停止后轮询 TC-001，任务应从 `CANCELLING` 进入 `CANCELED`。

### TC-011 停止前触发 savepoint

```bash
curl -X POST "${BASE_URL}/openapi/app/cancel" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${COPY_JOB_NAME}" \
  --data-urlencode "triggerSavepoint=true" \
  --data-urlencode "savepointPath=s3://eq-bigdata-cloud-test/flink/savepoints" \
  --data-urlencode "drain=false"
```

检查点：响应中 `status=success`。停止完成后可通过控制台内部记录确认生成 savepoint；OpenAPI 不再提供 latest 查询接口。

### TC-012 重启任务

```bash
curl --max-time 3900 -X POST "${BASE_URL}/openapi/app/restart" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${COPY_JOB_NAME}" \
  --data-urlencode "allowNonRestored=false" \
  --data-urlencode "drain=false"
```

检查点：响应中 `status=success`。该接口同步等待任务可启动后，再自动使用 latest checkpoint 启动；客户端超时时间应大于服务端等待停止超时时间。

### TC-013 全流程回归

该用例覆盖已验证的自动化部署主链路，建议作为版本发布前的最小回归：复制源任务，更新 SQL 或参数，构建，启动，等待 `RUNNING`，停止并触发 savepoint，最后等待任务进入 `CANCELED`。

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

async function waitForState(jobName, expected) {
  for (let i = 1; i <= 30; i++) {
    const app = await post('/openapi/app/get', { jobName });
    console.log(`poll=${i} state=${app.state} ${names[app.state] || ''} jobId=${app.jobId || ''}`);
    if (app.state === expected) return app;
    await new Promise((resolve) => setTimeout(resolve, 10000));
  }
  throw new Error(`app ${jobName} did not reach state ${expected}`);
}

const copied = await post('/openapi/app/copy', {
  srcJobName: process.env.SRC_JOB_NAME,
  dstJobName: process.env.COPY_JOB_NAME,
});

await post('/openapi/app/update', {
  jobName: process.env.COPY_JOB_NAME,
  mainClass: 'org.example.MainJob',
  flinkSql: "CREATE TABLE source_table (id INT) WITH ('connector'='datagen'); CREATE TABLE sink_table (id INT) WITH ('connector'='print'); INSERT INTO sink_table SELECT id FROM source_table;",
  args: '--env test --parallelism 2',
  dynamicProperties: '-Dexecution.checkpointing.interval=60000',
});

await post('/openapi/app/build', { jobName: process.env.COPY_JOB_NAME, forceBuild: 'true' });
await post('/openapi/app/build', { jobName: process.env.COPY_JOB_NAME, forceBuild: 'false' });
await post('/openapi/app/start', { jobName: process.env.COPY_JOB_NAME });
await waitForState(process.env.COPY_JOB_NAME, 5);
await post('/openapi/app/cancel', { jobName: process.env.COPY_JOB_NAME, triggerSavepoint: 'true' });
await waitForState(process.env.COPY_JOB_NAME, 9);
NODE
```

检查点：脚本无异常退出；复制任务进入 `RUNNING` 后被停止到 `CANCELED`。

## 异常测试用例

| 用例 ID | 接口 | 场景 | 预期结果 |
| --- | --- | --- | --- |
| TC-N001 | 任意 OpenAPI | 不传 `Authorization` | 请求被拒绝或返回认证失败 |
| TC-N002 | 任意 OpenAPI | 使用无效或旧 token | 请求被拒绝或返回认证失败 |
| TC-N003 | `POST /openapi/app/get` | `jobName` 不存在 | 返回失败信息，不得服务端异常崩溃 |
| TC-N004 | `POST /openapi/app/copy` | `dstJobName` 重复 | 返回失败信息，提示任务名不能重复 |
| TC-N005 | `POST /openapi/app/copy` | `srcJobName` 不存在 | 返回失败信息，不应创建无 Team 上下文的新任务 |
| TC-N006 | `POST /openapi/app/build` | 使用 `appId` 而不是 `jobName` | 参数校验失败，不应静默构建错误任务 |
| TC-N007 | `POST /openapi/app/start` | 请求从最新 checkpoint 启动但无 latest 记录 | 返回启动失败信息，任务不应进入错误的运行态 |
| TC-N008 | `POST /openapi/app/restart` | token 只有启动权限没有停止权限 | 请求被拒绝，不能停止任务 |
| TC-N009 | `POST /flink/savepoint/history` | 用 OpenAPI token 调内部非白名单接口 | 返回 OpenAPI unsupported，不应暴露内部接口 |

### TC-N001 缺少认证头

```bash
curl -X POST "${BASE_URL}/openapi/app/get" \
  -d "jobName=${JOB_NAME}"
```

检查点：请求不能成功读取任务详情。

### TC-N002 无效或旧 token

```bash
curl -X POST "${BASE_URL}/openapi/app/get" \
  -H "Authorization: invalid-token" \
  -d "jobName=${JOB_NAME}"
```

检查点：请求不能成功读取任务详情。该用例覆盖 token 解密和比对逻辑，避免旧的随机 IV 密文比较问题回归。

### TC-N003 查询不存在任务

```bash
curl -X POST "${BASE_URL}/openapi/app/get" \
  -H "Authorization: ${TOKEN}" \
  -d "jobName=not-exists-${COPY_JOB_NAME}"
```

检查点：返回可解析的错误响应，服务端日志不应出现未处理异常。

### TC-N004 复制任务名重复

```bash
curl -X POST "${BASE_URL}/openapi/app/copy" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "srcJobName=${SRC_JOB_NAME}" \
  --data-urlencode "dstJobName=${COPY_JOB_NAME}"
```

检查点：第一次复制成功后，再次用相同 `dstJobName` 调用应失败并提示任务名不能重复。

### TC-N005 复制不存在任务

```bash
curl -X POST "${BASE_URL}/openapi/app/copy" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "srcJobName=not-exists-${COPY_JOB_NAME}" \
  --data-urlencode "dstJobName=${COPY_JOB_NAME}-missing-source"
```

检查点：请求应失败，不能在缺少源任务 Team 上下文时创建新任务。

### TC-N006 构建接口误用 appId 参数

```bash
curl -X POST "${BASE_URL}/openapi/app/build" \
  -H "Authorization: ${TOKEN}" \
  -d "appId=1" \
  -d "forceBuild=true"
```

检查点：请求应返回参数校验失败，例如 `jobName is required`。OpenAPI 文档和调用方必须使用 `jobName`。

### TC-N007 无 latest checkpoint 时请求 checkpoint 启动

```bash
curl -X POST "${BASE_URL}/openapi/app/start" \
  -H "Authorization: ${TOKEN}" \
  --data-urlencode "jobName=${COPY_JOB_NAME}" \
  --data-urlencode "restoreFromLatestCheckpoint=true" \
  --data-urlencode "allowNonRestored=false"
```

检查点：请求返回启动失败信息，不应静默成功。

### TC-N008 重启权限不足

```bash
curl --max-time 3900 -X POST "${BASE_URL}/openapi/app/restart" \
  -H "Authorization: ${START_ONLY_TOKEN}" \
  --data-urlencode "jobName=${COPY_JOB_NAME}"
```

检查点：仅有 `app:start` 权限、没有 `app:cancel` 权限的 token 不能调用重启接口。

### TC-N009 OpenAPI token 调内部 savepoint history

```bash
curl -X POST "${BASE_URL}/flink/savepoint/history" \
  -H "Authorization: ${TOKEN}" \
  -d "appId=${INTERNAL_APP_ID}" \
  -d "teamId=${INTERNAL_TEAM_ID}" \
  -d "pageNum=1" \
  -d "pageSize=10"
```

检查点：请求应失败并提示 `Openapi unsupported: /flink/savepoint/history`。外部调用方不能依赖内部接口。
