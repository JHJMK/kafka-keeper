# Kafka Keeper

[English](README.md) | 简体中文

Kafka Keeper 是一个完全使用 Go 编写的原生桌面 Kafka 客户端，基于 [MyGo](https://github.com/egoist/mygo) 原生 UI 框架和 [franz-go](https://github.com/twmb/franz-go)，不依赖 HTML、JavaScript 或 WebView。

## 功能

- 管理多个 Kafka 连接，支持 TLS、SASL/PLAIN、SCRAM-SHA-256 和 SCRAM-SHA-512 认证。
- 通过系统原生文件选择器选择自定义 CA 证书；证书会复制到 Kafka Keeper 的应用数据目录。
- 通过 SSH 跳板机访问仅能在 VPC 内网访问的 Kafka 集群。
- 首次连接时确认并固定 SSH 主机指纹，后续连接会校验已保存的指纹。
- 在集群概览中查看 Broker、Topic、Partition、Consumer Group、成员、Offset 和 Lag。
- 在概览、Topics 和消费者组页面启用自定义秒数的自动刷新。
- 查看 Topic 分区信息，包括 Leader、副本、起始/结束 Offset 和消息数量。
- 查看 Topic 配置项。
- 不加入 Consumer Group 直接浏览 Topic 消息，支持按 Offset、时间点或时间范围查询。
- 浏览单个分区或全部分区，并可关闭消息条数上限。
- 按 Key 或 Header 查找消息，支持时间范围、Offset 范围、最大扫描数和最大结果数。
- 支持可选的 Key 子串匹配，以及 Header Key/Value 精确匹配；不会搜索消息 Value。
- 发送消息时可使用可编辑、带前缀过滤的 Topic 下拉框，并设置 Key、Value、Headers、压缩方式和时间戳。
- 支持使用 sticky partitioner 自动分区，也可以手动指定分区。
- 发送消息支持 none、gzip、snappy、lz4 和 zstd 压缩方式。
- 消费者组支持仅查看带有活跃成员的 Group。
- 查看消费者组状态、成员、分配关系、已提交 Offset 和 Lag。
- 新建真实加入 Consumer Group 的消费会话，支持从最早/最新位置重置、自动提交、手动提交、暂停、继续和停止。
- 天蓝白配色的原生桌面界面，使用圆角面板和系统原生日期/时间输入控件。

## 环境要求

- Go 1.27 或更高版本
- macOS、Windows 或 Linux

## 开发运行

以开发模式运行应用：

```bash
GOTOOLCHAIN=auto go run github.com/egoist/mygo/cmd/mygo@v0.7.3 dev .
```

运行测试与静态检查：

```bash
make test
GOTOOLCHAIN=auto go test -race ./...
GOTOOLCHAIN=auto go vet ./...
```

## 构建与打包

打包所有支持的平台：

```bash
make package
```

仅覆盖本次构建的应用版本：

```bash
make package VERSION=1.2.3
```

`VERSION` 会临时更新 `mygo.json`，打包结束后自动恢复原文件；不传该参数时使用 `mygo.json` 中的版本。

打包指定平台或平台组合：

```bash
make package PLATFORMS=darwin/universal
make package PLATFORMS=darwin/universal,linux/amd64
```

跳过测试直接构建：

```bash
make build
```

默认包含 macOS Universal（Apple Silicon 与 Intel）、Windows amd64/arm64，以及 Linux amd64/arm64。产物输出到 `build/`，包括 macOS `.app`/`.dmg`、Windows 可执行文件/安装程序，以及 Linux 可执行文件、`.deb` 和 `.tar.gz`。生成 Windows 安装程序需要 NSIS；macOS 正式发布需要配置代码签名和公证。

## 本地数据与安全

应用数据默认保存在：

```text
~/kafka-keeper/
```

主要文件包括：

```text
connections.json
credentials.json
ca-certificates/
```

Kafka 与 SSH 密码保存在 `credentials.json` 中。该文件通过原子方式写入，权限为 `0600`，不依赖操作系统钥匙串。凭据文件本身没有加密，安全性取决于本地文件权限和用户账户安全。

TLS 默认验证服务端证书。选择自定义 CA 后，Kafka Keeper 会将证书复制到受管理的 `ca-certificates/` 目录，避免连接依赖原证书路径。

请勿提交、共享或泄露应用数据目录，其中可能包含集群地址和凭据。

SSH 代理目前仅支持密码认证，不支持私钥、SSH Agent 或多跳代理。启用 SSH 代理时，Bootstrap Servers 必须填写从 SSH 主机网络可访问的地址。

## 使用说明

1. 在左侧添加 Kafka 连接并验证连通性。
2. 在 **Topics** 中查看分区与配置、浏览消息，或查找消息 Key 和 Header。
3. 在 **消费者组** 中查看成员和 Lag，或新建真实加入 Group 的消费会话并提交 Offset。
4. 在 **发送消息** 中选择或过滤 Topic，填写消息内容后发送。

同一 Kafka 连接同时只允许一个活动消费会话，避免 Offset 提交和消费状态相互干扰。
