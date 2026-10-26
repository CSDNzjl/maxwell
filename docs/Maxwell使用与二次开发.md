# Maxwell 使用与二次开发

本文档覆盖本仓库 Maxwell 1.46.0 的本地跑通（stdout）与自定义 HTTP Producer（推送到本机 Spring `8088/maxwell`）二次开发。

## 目录

1. [基本使用（stdout）](#1-基本使用stdout)
2. [二次开发过程（HTTP POST）](#2-二次开发过程http-post)
3. [代码解释](#3-代码解释)
4. [对接 8088 启动方式](#4-对接-8088-启动方式)
5. [常见问题](#5-常见问题)

---

## 1. 基本使用（stdout）

本机用 **stdout** 生产者把 MySQL binlog 变更打印到终端，方便先验连通性。

### 1.1 环境要求

- **JDK 17**：`JAVA_HOME` 设为 `D:\jdk`（不要用 JDK 8，否则会报 `无效的标记: --release`）
- 打包命令示例：

```powershell
$env:JAVA_HOME = "D:\jdk"
$env:PATH = "D:\jdk\bin;D:\apache-maven-3.9.12\bin;" + $env:PATH
cd C:\Users\16899\Desktop\maxwell\maxwell
mvn -B clean package "-DskipTests=true" "-Dmaven.javadoc.skip=true" "-Dgpg.skip=true"
```

产物目录：`target\maxwell-1.46.0\`（含 jar 与 `lib\`）。

### 1.2 MySQL 前置条件

1. 开启 binlog，格式为 ROW（建议 `binlog_row_image=FULL`），并设置独立的 `server_id`。
2. 创建 Maxwell 账号并授权（若用户已存在会报 Error 1396，改用 `ALTER USER` 即可）：

```sql
CREATE USER 'maxwell'@'%' IDENTIFIED BY '你的密码';
GRANT ALL ON maxwell.* TO 'maxwell'@'%';
GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'maxwell'@'%';
FLUSH PRIVILEGES;
```

3. **MySQL 端口**：不写 `--port` 时默认连接 **3306**；若 MySQL 改过端口再加 `--port=实际端口`。
4. **Maxwell 端口**：日常 stdout / Kafka 用法**不需要**给 Maxwell 配置监听端口；它是客户端进程，读 binlog 再往外发数据。

### 1.3 启动（推荐：PowerShell + java -cp）

在仓库根目录执行：

```powershell
$env:JAVA_HOME = "D:\jdk"
cd C:\Users\16899\Desktop\maxwell\maxwell
java -cp "target\maxwell-1.46.0.jar;target\lib\*;target\lib\kafka-clients\*" `
  com.zendesk.maxwell.Maxwell `
  --user=maxwell `
  --password=xxxx `
  --host=127.0.0.1 `
  --producer=stdout
```

说明：

- `--host` 换成实际 MySQL 地址
- `--producer=stdout`：变更 JSON 打到标准输出
- 需要指定 MySQL 端口时追加：`--port=3307`

### 1.4 可选：Git Bash 使用 bin/maxwell

若已解压发行包或 `target` 下有完整目录结构，在 **Git Bash** 里也可以：

```bash
export JAVA_HOME="/d/jdk"
cd /c/Users/16899/Desktop/maxwell/maxwell
bin/maxwell --user=maxwell --password=你的密码 --host=127.0.0.1 --producer=stdout
```

路径以你实际的 `bin/maxwell` 位置为准。Windows 上也可以用配置文件：`bin/maxwell --config config.properties`。

### 1.5 如何确认跑通

1. Maxwell 启动无报错并保持运行。
2. 在被监听的库表执行 `INSERT` / `UPDATE` / `DELETE`。
3. 终端应出现对应 JSON 变更记录。

验证通过后，可将 `--producer=stdout` 换成 `kafka`、`redis`、`rabbitmq` 等，或使用下文的 **custom HTTP** 生产者。

---

## 2. 二次开发过程（HTTP POST）

目标：Maxwell 每条 CDC 事件，以表单字段 `message`（URL 编码后的 JSON）POST 到本机 Spring：

```java
@PostMapping("/maxwell")
public void printMessageForMaxwell(String message) {
    System.out.println(message);
}
```

注意：该接口参数是普通 `String message`（无 `@RequestBody`），因此请求必须是  
`Content-Type: application/x-www-form-urlencoded`，字段名 `message`，**不要**发 raw JSON body。

### 2.1 步骤概览

1. 在本仓库实现 `ProducerFactory` + `AbstractProducer`（已完成，见下一节文件路径）。
2. `mvn package` 把新类打进 `maxwell-*.jar`。
3. 本机先启动 Spring，监听 `8088`，提供 `POST /maxwell`。
4. Maxwell 用 `--producer=custom` 指向工厂类并启动。
5. 改库表数据，观察 Spring 控制台打印 JSON。

### 2.2 新增文件

| 文件 | 作用 |
|------|------|
| `src/main/java/com/zendesk/maxwell/producer/HttpPostProducer.java` | 真正发 HTTP 的生产者 |
| `src/main/java/com/zendesk/maxwell/producer/HttpPostProducerFactory.java` | 工厂，供 `--custom_producer.factory` 反射加载 |

配置项（写在 **config.properties**；前缀 `custom_producer.` / `custom_producer_` 会进入 `customProducerProperties`。CLI 仅支持 `--custom_producer.factory`）：

| 属性 | 含义 | 默认 |
|------|------|------|
| `custom_producer.url` | POST 目标 URL | `http://127.0.0.1:8088/maxwell` |

### 2.3 为何用 custom 而不是改内置生产者

Maxwell 内置 stdout / kafka / redis 等，没有 HTTP。官方扩展点是  
`--producer=custom` + 自定义 `ProducerFactory`，这样不改核心调度，只替换“往哪发”。

---

## 3. 代码解释

源码里的类注释与方法注释已用中文写清：工厂方法职责、表单字段 message 与 Spring 绑定、仅 HTTP 成功后 setPosition、以及 custom_producer.url 的读取方式。以下为结构说明。

### 3.1 `HttpPostProducerFactory`

- 实现接口 `com.zendesk.maxwell.producer.ProducerFactory`
- 唯一方法：`createProducer(MaxwellContext)` → `new HttpPostProducer(context)`
- Maxwell 启动时按全限定类名反射创建工厂，再创建 Producer

### 3.2 `HttpPostProducer.push(RowMap)`

处理流程与 `FileProducer` / `StdoutProducer` 同类：

1. **`shouldOutput(outputConfig)`**：过滤不需要输出的行（如 bootstrap 内部行等）；不输出时仍 `setPosition`，避免位点卡住。
2. **`toJSON(outputConfig)`**：按 Maxwell 输出配置序列化为 JSON 字符串；为 `null` 则推进位点并返回。
3. **HTTP POST**：构造 `message=<URLEncoder.encode(json, UTF-8)>`，`Content-Type: application/x-www-form-urlencoded; charset=UTF-8`，发到配置的 URL。
4. **仅成功时 `context.setPosition(r)`**：HTTP 2xx 才推进 binlog 位点；失败抛异常且不推进，便于重启后重试（避免丢事件）。
5. 成功/失败时更新 metrics 计数（与内置生产者一致）。

### 3.3 与 Spring 参数绑定的对应关系

| Maxwell 发出 | Spring 接收 |
|--------------|-------------|
| POST body：`message=...`（form） | 方法参数 `String message` |
| JSON 本身作为 `message` 的值（已 URL 编码） | `System.out.println(message)` 打印解码后的 JSON 文本 |

---

## 4. 对接 8088 启动方式

### 4.1 先启动 Spring

确保本机有接口：`POST http://127.0.0.1:8088/maxwell`，参数名 `message`。可用 Postman / curl 先自测：

```powershell
curl -X POST "http://127.0.0.1:8088/maxwell" `
  -H "Content-Type: application/x-www-form-urlencoded; charset=UTF-8" `
  --data-urlencode "message={\"hello\":\"maxwell\"}"
```

Spring 控制台应打印：`{"hello":"maxwell"}`。

### 4.2 再启动 Maxwell（custom HTTP）

```powershell
$env:JAVA_HOME = "D:\jdk"
cd C:\Users\16899\Desktop\maxwell\maxwell
java -cp "target\maxwell-1.46.0.jar;target\lib\*;target\lib\kafka-clients\*" `
  com.zendesk.maxwell.Maxwell `
  --user=maxwell `
  --password=你的密码 `
  --host=127.0.0.1 `
  --producer=custom `
  --custom_producer.factory=com.zendesk.maxwell.producer.HttpPostProducerFactory
```

说明：命令行**只认** `--custom_producer.factory`；`custom_producer.url` 等其它键**不能**写成 `--custom_producer.url=...`（会报 `UnrecognizedOptionException`）。

- 用默认地址 `http://127.0.0.1:8088/maxwell` 时：上面这样启动即可，不必写 url。
- 要改 URL：写到 `config.properties`（或任意 `--config` 文件）里，例如：

```properties
custom_producer.url=http://127.0.0.1:8088/maxwell
```

再加 `--config=config.properties`（factory 也可写进同一文件）。

### 4.3 端到端验证

1. Spring 已监听 8088。
2. Maxwell 以 custom 工厂启动成功（日志里可见 `HttpPostProducer will POST ...`）。
3. 对监听库表做 DML。
4. Spring 控制台打印出 Maxwell JSON；Maxwell 侧无持续 HTTP 报错。

---

## 5. 常见问题

| 现象 | 原因 / 处理 |
|------|-------------|
| `无效的标记: --release` | 仍在用 JDK 8，改用 `D:\jdk` |
| `Error 1396 ... CREATE USER` | 用户已存在，用 `ALTER USER` 或 `DROP USER` |
| 打包 gpg / javadoc 失败 | 加 `-Dgpg.skip=true`、`-Dmaven.javadoc.skip=true`（绿盾可能干扰 javadoc 读源码） |
| 测试失败 | 集成测试依赖真实 MySQL；日常打包用 `-DskipTests=true` |
| HTTP 连不上 / 4xx 5xx | 先确认 Spring 已启动；Maxwell **不会**在失败时推进位点 |
| 公司绿盾 | 部分源码磁盘上是密文；用 IDEA 打开/编译一般正常；记事本直接打开 md 可能异常 |

### 补充说明

- Maxwell **自身**默认不对外提供 Web 服务；stdout / custom HTTP 模式都不必配置“Maxwell 端口”。
- 二次开发类已放在主工程 `com.zendesk.maxwell.producer` 包内，重新 `package` 后即可直接用，无需再往 `lib/` 另丢 jar。
