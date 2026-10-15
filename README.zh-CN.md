# wecom-dot-bridge：企业微信 ⇄ 你已有的 ChatGPT dot

[English](README.md) | 简体中文

通过一个专用的企业微信 AI 机器人，将所有者私聊中的消息
（文本、图片、文件、引用回复）转发到**已有的** ChatGPT dot 消息房间，
再将 dot 的回复和文件发回同一个企业微信会话。

> **非官方项目。** 本项目与 OpenAI、腾讯均无关联。它使用的 ChatGPT 接口没有公开文档，
> 接口行为来自对 ChatGPT 应用和两个非官方客户端的观察。这些接口随时可能变更或失效，
> 自动化使用也可能违反相关服务条款。请仅使用你自己的账号，并自行承担风险。

## 项目组成

- 一个 Python 3.13 asyncio 进程（`python3 -m dot_bridge serve`）。依赖 Debian 的
  `python3-aiohttp` 和 `python3-cryptography`，后者用于解密企业微信媒体文件。
- **企业微信：** 通过官方 `wss://openws.work.weixin.qq.com` 长连接接入 AI 机器人。
  不需要公网回调地址，也不需要开放入站端口。协议处理和扫码注册流程改编自 Hermes Agent，
  采用 MIT 许可证，详见 `THIRD_PARTY_NOTICES.md`。
- **ChatGPT：** 桥接服务使用独立的 Codex OAuth 设备登录。
  不要从其他 Codex 客户端复制令牌，刷新令牌只能使用一次。
- **dot：** 使用 `chatgpt.com/backend-api` 下没有公开文档的消息接口
  （`/tbo`、`/messaging/rooms/{room}/messages`），以及 `/celsius/ws/user` 提供的实时更新连接。
  协议实现参考了两个非官方客户端：
  [pi-plugins dot-live.ts](https://github.com/gunba/pi-plugins/blob/b4c064050fbc0f445cd56ca838b590920fecf29f/pi-desk/src/host/dot-live.ts)
  和 [beeper chatgpt-dots signals.go](https://github.com/beeper/chatgpt-dots/blob/0c75b6ffae91d36e9e637307b8d0adc8c2eb0ad2/pkg/chatgpt/signals.go)。
  实时连接的 URL 含有认证信息，绝不会写入日志。桥接服务不会创建房间。
  如果 OpenAI 更改这些接口，服务会停止转发并报告问题，不会猜测新的接口行为。

## 运行行为

| 情况 | 处理方式 |
| --- | --- |
| 已绑定所有者以外的用户，或任何群聊 | 静默忽略 |
| 所有者发送文本或语音转写 | 持久化入队，按企业微信 `msgid` 去重，再使用固定的 `request_id`/`idempotency_token` 提交一次。严格遵循先进先出顺序。 |
| HTTP 200/201 | 已送达 |
| 未能建立连接，包括 DNS、连接被拒绝、TLS、连接超时 | 消息尚未发送，队列暂停 30 秒，并保持原有顺序 |
| HTTP 401 | 刷新令牌一次，再重试一次。401 表示消息未被接受。如果新令牌仍被拒绝，则清除登录状态，让消息继续等待，并向所有者发送一次提示，要求执行 `/login`。 |
| HTTP 429 | 消息未被接受，队列暂停 30 秒，并保持原有顺序 |
| 其他 4xx | 消息被拒绝，通知所有者，可用 `/resend` 重发或用 `/discard` 丢弃 |
| 连接建立后超时、5xx、意外错误、发送过程中崩溃 | 发送结果未知。服务会检查房间中是否存在**明确的送达证据**，即已绑定用户发送的、携带完全相同 `request_id` 的消息。实测接口会在我们提交的每条消息中返回该字段。若找到，则认定已送达；否则标记为**结果不确定**，向所有者发送包含消息预览的通知，且**绝不自动重发**。后续消息不会被阻塞，队列暂停 30 秒。 |
| dot 回复 | 由推送触发。服务监听 ChatGPT 的用户级连接 `/celsius/ws/user`，以后台在线状态订阅 `calpico-chatgpt-messaging` 主题。每次收到消息新增或更新事件、每次订阅或重新订阅，以及连接断开时，都会通过 REST 接口读取房间中的实际消息。连接正常时，常规轮询仅每 120 秒执行一次，用于补漏；dot 消息尚未稳定时，间隔为 2 秒。连接不可用时，服务退回轮询：每条消息送达后立即检查一次；有活动后的 15 分钟内每 2 秒检查一次，其他时间每 60 秒检查一次。按 dot 的 aeon 成员 ID 筛选消息。未完成的 dot 消息会被持久化跟踪，直到完成，即使它们已不在最新一页中。如果最新一页未覆盖正在跟踪的消息，服务会向前翻页，每轮最多读取 10 页，下轮继续。回复完成后会固定其内容并写入持久化发件箱。完成条件为 0.5 秒未修改，或连续两次轮询文本不变，且不处于 `in_progress` 状态；空内容最多等待 2 分钟。企业微信连接正常时，发件箱按顺序发送。如果前一条回复仍在生成，已完成的回复最多等待 10 分钟。每段不超过 4000 个字符且不超过 12000 字节，发送速率遵守企业微信每个会话每分钟 30 条消息的限制。分段进度会持久化，失败或重启后从失败的分段继续，最多只有这一段可能重复。只有在企业微信明确拒绝 5 次、每次间隔 30 秒后，才会放弃该回复。 |
| 所有者在企业微信发送图片、文件或图文消息 | 与文本一样入队。在记录发送意图之前，先从企业微信下载每个文件，使用消息中的密钥进行 AES-256-CBC 解密，从 `Content-Disposition` 获取文件名并进行百分号解码和长度限制，再上传到 dot 房间（`POST /messaging/rooms/{room}/files`），并持久化其 `file_id`。重试时复用这些 ID。随后使用 `attachments: [{type: "file", file_id}]` 提交消息；如果没有文本，则使用文件名作为消息文本，格式为 "📎 name"。下载或上传失败时，每隔 30 秒重试一次，共重试 3 次，之后报告错误。超过 20 MB 的文件会被拒绝，并发送提示。 |
| dot 发送文件 | 发件箱将文件引用与已固定的文本一并保存。先发送文本，如有，再逐个重新获取文件：先请求 `GET /messaging/rooms/{room}/files/{id}`，再访问签名的 `*.oaiusercontent.com` URL，访问时不携带账号请求头。随后以 512 KB 分块上传到企业微信，以 `image` 发送不超过 10 MB 的 PNG/JPEG/GIF，其他文件以 `file` 发送。每个文件的进度都会持久化。超过 20 MB 或尝试 5 次后仍失败的文件，会替换为提示用户前往 ChatGPT 查看文件的通知，以免阻塞后续回复。 |
| 所有者在企业微信引用回复 | 企业微信传递的是被引用的*内容*，而非消息 ID。服务会记住与每条 dot 消息对应的文本，包括已转发的 dot 回复和所有者已送达的消息。匹配时只保留字母和数字，因此不受 Markdown 和空白字符影响，再按前缀映射引用。匹配成功后，发送 dot 原生引用回复（`"reply_to": {"message_id": …}`，已通过实际接口验证）。否则，将引用作为上下文放在消息上方，格式为 `> excerpt`，图片或文件则为 `> [image]`/`> [file]`。 |
| dot 回复较早的消息 | 如果 dot 引用的不是所有者最新的一条消息，发到企业微信的消息会以一行 `> excerpt` 开头。 |

桥接服务的提示默认使用美式英语。发送 `/lang zh` 可切换为简体中文，发送 `/lang en` 可切回英语。
传给 dot 的引用上下文、文件占位文本始终使用英语。
企业微信命令仅限所有者私聊使用，每个 `msgid` 只执行一次：
`/login`（仅在服务报告 ChatGPT 登录失效时使用，登录仍有效时会拒绝执行）、
`/status`、`/dots`、`/use <dot_id>`、`/lang en|zh`、
`/pending`（列出结果不确定或被拒绝的消息）、
`/resend`（按顺序使用原有幂等令牌重发这些消息，且只能发给各条消息最初发送到的 dot）、
`/discard`、`/help`。其他文本，包括未知的 `/…` 命令，都会转发给 dot。

## 隔离措施，参考部署使用 Incus

`deploy/` 面向 Incus 主机，开发环境为加载了 Incus 和 ZFS 系统扩展的 Flatcar。
桥接服务使用独立环境，不与其他工作负载共享资源：

- Incus 项目 `dot`：启用受限模式和独立 ID 映射，禁止嵌套，只允许受管理的磁盘和网卡，
  禁止代理设备，最多一个容器。项目配额为 2 个 CPU、1 GiB 内存、1024 个进程，
  在存储池 `agents` 上最多使用 20 GiB。
- 网络 `dot0`（10.251.0.1/24）使用独立 ACL `dot-egress`。拒绝访问所有私有地址、
  链路本地地址和组播地址段，允许公网 TCP 80/443，以及到 223.5.5.5 / 1.1.1.1 的 DNS 请求，
  不接受入站连接。Incus 始终允许容器访问其所在网桥网关上的 DHCP/DNS 服务（ACL 无法阻止）；
  桥接不使用该服务，`resolv.conf` 中只配置了上述两个公共解析器。
- 容器 `dot-bridge`（Debian 13）使用配置文件 `dot-bridge`：1 个 CPU、512 MiB 内存、
  8 GiB 根磁盘。自定义 ZFS 卷 `dot-state` 挂载到 `/var/lib/dot-bridge`，容量为 2 GiB，
  每天 03:00 创建快照，保留 30 天。
- systemd 单元 `dot-bridge.service` 以非特权用户 `dot-bridge` 运行，采用严格沙箱：
  无 capabilities，系统只读，仅允许写入 `/var/lib/dot-bridge`，内存上限为 256 MiB。

状态保存在 `/var/lib/dot-bridge`，目录权限为 0700：

- `bridge.sqlite3`：保存队列、回复记录、所有者与 dot 的绑定关系。消息文本为明文，
  已送达或失败的记录保留 30 天后清理。
- `secrets/wecom.json`、`secrets/openai.json`：权限为 0600，绝不写入日志。

## 安装与升级

在已拉取本仓库代码的工作站上执行以下命令。`HOST` 是可通过 SSH 访问的 Incus 主机：

```sh
scp deploy/incus-create.sh HOST:/tmp/
ssh HOST sudo sh /tmp/incus-create.sh
tar czf - dot_bridge/*.py deploy | ssh HOST \
  'sudo incus exec dot-bridge --project dot -- sh -c "rm -rf /root/src && mkdir /root/src && tar xzf - -C /root/src && sh /root/src/deploy/provision.sh"'
```

- `incus-create.sh` 默认使用名为 `agents` 的 ZFS 存储池，如需更换请设置 `POOL`。
  容器的独立 ID 映射要求 root 的 `/etc/subuid` 和 `/etc/subgid` 中存在一个可用的、
  包含 65,536 个 ID 的区段。脚本会检查这一条件，缺失时会给出说明。
- 在同一主机上部署第二个相互独立的桥接时，需为其指定独立的项目、网络、ACL 和子网：
  `sudo PROJECT=dot2 NETWORK=dot2net SUBNET=10.252.0 sh /tmp/incus-create.sh`
  （ACL 默认为 `dot2-egress`）。此处及下文的所有命令都需改用 `--project dot2`。
  它需要自己的企业微信机器人（`enroll-wecom`）；两个桥接共用一个机器人会相互断开连接。
- `provision.sh` 可重复执行，并将上一版代码保存在 `/opt/dot-bridge.old`。
- 不使用 Incus 时，只要环境具备 Python 3.11+、aiohttp 和 cryptography，
  就可以运行 `DOT_BRIDGE_STATE=/path python3 -m dot_bridge serve`。
  参考部署采用的沙箱配置见 `deploy/dot-bridge.service`。

## 首次配置，所有者操作

所有命令均在容器内运行，即 `sudo incus exec dot-bridge --project dot -- …`。
包装脚本会自动切换为 `dot-bridge` 用户。

1. 运行 `dot-bridge enroll-wecom`，在企业微信中扫码或打开终端输出的链接。
   这会创建一个新的 AI 机器人。正在运行的服务会在 10 秒内加载它。
2. 运行 `dot-bridge claim`，在 15 分钟内通过企业微信私聊向新机器人发送终端输出的
   `/claim <code>`。发送者将成为唯一的所有者。
3. 向机器人发送 `/login`，打开链接，使用拥有该 dot 的 ChatGPT 账号登录并输入验证码。
   如果账号下只有一个 dot，会自动绑定；否则使用 `/dots` 和 `/use <dot_id>` 选择。
   也可以在终端运行 `dot-bridge login` 完成登录。

## 运维

- 健康检查：`dot-bridge status`。服务最近一次心跳距今不足 90 秒时，退出码为 0，
  同时输出企业微信连接、登录、绑定状态和队列计数。日志查看命令为 `journalctl -u dot-bridge`。
  消息文本和令牌绝不会写入日志。
- 更换所有者：先执行 `systemctl stop dot-bridge`，再执行 `dot-bridge reset-owner`。
  服务运行时会拒绝重置。重置会清除所有者、dot 绑定和 ChatGPT 登录状态，
  并丢弃尚未发送的消息及尚未送达的回复，确保新所有者不会继承这些数据。
  随后启动服务并运行 `dot-bridge claim`。
- 退出码 75 表示企业微信报告另一个客户端使用了同一机器人的凭据，
  或另一个桥接进程使用了同一状态目录。systemd 不会自动重启，
  `dot-bridge status` 会显示 `fatal`。请先停止另一个客户端，再执行
  `systemctl restart dot-bridge`。自动重连会导致两个客户端互相踢下线。
- 重新注册机器人：停止服务，删除 `secrets/wecom.json`，运行 `enroll-wecom`，再启动服务。
- 撤销 ChatGPT 访问权限：删除 `secrets/openai.json`，服务会提示执行 `/login`，
  同时在 ChatGPT 账号设置中退出该会话。
- 备份：ZFS 卷快照具备崩溃一致性，SQLite 使用 WAL 和 `synchronous=FULL`。
  如需应用一致性备份，先停止服务，再执行
  `incus storage volume export agents dot-state --project dot <file>`。
  恢复时先导入新卷并测试。
- 回滚代码：在容器内执行 `mv /opt/dot-bridge /opt/dot-bridge.bad &&
  mv /opt/dot-bridge.old /opt/dot-bridge && systemctl restart dot-bridge`。
- 完全卸载：先执行 `incus delete -f dot-bridge --project dot`，
  再删除 `dot-state` 卷、配置文件、项目、网络 `dot0` 和 ACL `dot-egress`。
  主机上的其他工作负载不依赖这些资源。

## 测试

```sh
python3 -m unittest discover -s tests -v
```

测试套件离线运行。大多数测试使用模拟的 HTTP 和企业微信传输。
集成测试包括 `test_aiohttp_integration.py`，以及 `test_live.py` 和 `test_files.py`
中的部分测试，它们让实际的 aiohttp 代码连接本地服务器，覆盖分块响应体、重定向、
multipart 上传、企业微信协议交互和实时连接协议交互。缺少 aiohttp 或 cryptography 时，
这些测试会跳过。

## 已知限制

- dot 接口没有公开文档，随时可能变更。接口行为已于 2026-10-15 使用真实账号验证，
  详见 `docs/dot-api.md`。房间成员信息只能从 `/tbo` 列表取得，因此 dot 身份也依赖此列表，
  不能从 `GET /tbo/{id}` 获取。
- 两个方向的文件上限均为 20 MB，这是两端平台的限制。语音以转写文本转发，
  不转发视频。dot 的非文件附件，如链接、卡片，会以提示形式告知用户。
- 仅面向单人使用：一个所有者，只支持私聊。设计上不提供多用户、容量管理或对抗恶意负载的功能。
- 出站访问仅允许公网 TCP 80/443 和两个 DNS 解析器，并非按域名设置的允许列表。
  对端包括经 Cloudflare 提供服务的 chatgpt.com、腾讯企业微信接口和 Debian 软件源，
  它们使用规模较大且持续变化的 CDN 地址段。容器只运行此服务，不运行 agent 或不受信任的代码。
  如果这一点发生变化，下一步应部署可过滤流量的出站代理。
