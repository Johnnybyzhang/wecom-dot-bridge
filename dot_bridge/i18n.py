"""Owner-facing bridge messages. Default en-US; `/lang zh` switches to zh-CN.

Only the bridge's own notices to the owner are translated. Text that reaches the dot
(quote context, file placeholders) stays English, the owner's dot language.
"""
from __future__ import annotations

LANGS = ("en", "zh")
DEFAULT = "en"

M = {
    "help": (
        "**dot bridge commands**\n"
        "- Plain text, images and files go to your dot; its replies come back here\n"
        "- Quote a message to reply to it\n"
        "- `/login`: sign in to ChatGPT again (only when the bridge says it expired)\n"
        "- `/status`: bridge status\n"
        "- `/dots`, `/use <dot_id>`: list / choose a dot\n"
        "- `/pending`: messages not confirmed or rejected\n"
        "- `/resend`: resend them in order (same idempotency key)\n"
        "- `/discard`: drop them\n"
        "- `/lang en` | `/lang zh`: language of these notices\n"
        "- `/help`: this help",
        "**dot 桥接命令**\n"
        "- 直接发文字、图片或文件：转发给 dot，dot 的回复会发回这里\n"
        "- 引用某条消息即可回复它\n"
        "- `/login`：登录失效时重新登录 ChatGPT\n"
        "- `/status`：查看状态\n"
        "- `/dots`、`/use <dot_id>`：列出 / 选择 dot\n"
        "- `/pending`：列出未确认送达或被拒绝的消息\n"
        "- `/resend`：按顺序重发这些消息（同一幂等标识）\n"
        "- `/discard`：放弃这些消息\n"
        "- `/lang en` | `/lang zh`：通知语言\n"
        "- `/help`：帮助"),
    "lang_set": ("Notices are now in English.", "通知语言已切换为中文。"),
    "lang_usage": ("Usage: `/lang en` or `/lang zh`", "用法：`/lang en` 或 `/lang zh`"),
    "unsupported": ("Only text, images and files can be forwarded to your dot.",
                    "目前只支持文字、图片和文件。"),
    "too_long": ("Message too long (over {max} characters). Please split it.",
                 "消息过长（>{max} 字），请拆分后再发。"),
    "queued_unbound": ("Queued. It will be sent once you `/login` and a dot is connected.",
                       "消息已排队。登录（`/login`）并连接 dot 后会按顺序发送。"),
    "claimed": ("✅ You are now this bot's only user.", "✅ 已绑定你为此机器人的唯一使用者。"),
    "next_login": ("Next: send `/login` to sign in to ChatGPT.", "下一步：发送 `/login` 登录 ChatGPT。"),
    "already_claimed": ("This bot is already yours; no need to claim it again.",
                        "此机器人已经绑定了你，无需再次认领。"),
    "pending_title": ("**Not delivered:**", "**未送达的消息：**"),
    "pending_uncertain": ("unconfirmed", "未确认"),
    "pending_rejected": ("rejected", "被拒绝"),
    "pending_other_dot": (", for another dot, won't be resent", "，原属其他 dot，不会重发"),
    "pending_footer": ("`/resend` resends all in order, `/discard` drops them.",
                       "`/resend` 按顺序全部重发，`/discard` 全部放弃。"),
    "pending_none": ("No unconfirmed or rejected messages.", "没有未确认或被拒绝的消息。"),
    "resend_n": ("Resending {n} message(s) in order.", "将按顺序重发 {n} 条消息。"),
    "resend_none": ("Nothing to resend.", "没有需要重发的消息。"),
    "discard_n": ("Dropped {n} message(s).", "已放弃 {n} 条消息。"),
    "discard_none": ("Nothing to drop.", "没有可放弃的消息。"),
    "login_valid": ("Your ChatGPT sign-in is still valid; `/login` isn't needed. "
                    "I'll tell you if it ever expires.",
                    "ChatGPT 登录仍然有效，无需 `/login`。只有登录失效时我会提示你重新登录。"),
    "login_running": ("A sign-in is already in progress; finish it in your browser.",
                      "登录已在进行中，请完成浏览器中的授权。"),
    "login_unavailable": ("Can't start sign-in right now. Please try again later.",
                          "暂时无法开始登录，请稍后再试。"),
    "login_prompt": ("Within 15 minutes, open {url}\nSign in with the ChatGPT account that has your dot "
                     "and enter the code: **{code}**",
                     "请在 15 分钟内打开 {url}\n登录要连接 dot 的 ChatGPT 账号，并输入代码：**{code}**"),
    "login_expired": ("The sign-in expired. Send `/login` again when needed.",
                      "登录已过期。需要时请再次发送 `/login`。"),
    "login_failed": ("Sign-in failed. Please send `/login` again.", "登录失败，请再次发送 `/login`。"),
    "login_other_account": ("⚠️ That account doesn't own the previously connected dot; it was unbound.",
                            "⚠️ 登录的账号与之前绑定的 dot 不一致，已解除旧绑定。"),
    "login_ok": ("✅ Signed in to ChatGPT.", "✅ ChatGPT 登录成功。"),
    "dots_unreadable_retry": ("Can't read your dots; try `/dots` later.", "无法读取 dot 列表，请稍后发送 `/dots`。"),
    "dots_none": ("This account has no dot yet. Create or open one in ChatGPT first.",
                  "这个账号下没有 dot。请先在 ChatGPT 中创建/打开 dot。"),
    "not_logged_in": ("Not signed in; send `/login`.", "尚未登录，请发送 `/login`。"),
    "dots_unreadable": ("Can't read your dots right now. Please try again later.",
                        "无法读取 dot 列表，请稍后再试。"),
    "dots_title": ("**Your dots:**", "**你的 dot：**"),
    "dots_empty": ("(none)", "（无）"),
    "dots_footer": ("Choose one with `/use <dot_id>`.", "用 `/use <dot_id>` 选择。"),
    "use_usage": ("Usage: `/use <dot_id>`", "用法：`/use <dot_id>`"),
    "bind_failed": ("Can't connect that dot: {error}", "无法绑定该 dot：{error}"),
    "bind_no_room": ("it has no messaging room yet; open it once in ChatGPT",
                     "它还没有消息房间，请先在 ChatGPT 中打开一次"),
    "bind_no_member": ("its room has no dot member", "房间中没有 dot 成员"),
    "bind_not_found": ("this account has no dot with that id; send `/dots`",
                       "这个账号下没有该 id 的 dot，请发送 `/dots`"),
    "bind_other": ("ChatGPT returned an unexpected response", "ChatGPT 返回了意外的结果"),
    "state_ready": ("waiting", "待发送"),
    "state_sending": ("sending", "发送中"),
    "state_delivered": ("delivered", "已送达"),
    "state_uncertain": ("unconfirmed", "未确认"),
    "state_failed": ("rejected", "被拒绝"),
    "chatgpt_unreachable": ("Can't reach ChatGPT right now. Please try again later.",
                            "暂时无法连接 ChatGPT，请稍后再试。"),
    "bound": ("✅ Connected to dot “{name}”. Everything you send me now goes to it.",
              "✅ 已连接到 dot「{name}」。现在发给我的文字都会转给它。"),
    "need_login": ("Sign-in to ChatGPT is needed to forward messages; send `/login`.",
                   "需要登录 ChatGPT 才能转发，请发送 `/login`。"),
    "login_invalid": ("Your ChatGPT sign-in expired or was refused; send `/login`.",
                      "ChatGPT 登录已失效或被拒绝，请发送 `/login`。"),
    "dot_changed": ("Message “{preview}” was meant for another dot and won't be sent to this one. "
                    "`/discard` clears it.",
                    "消息「{preview}」原本发往另一个 dot，已停止，不会发给当前 dot。`/discard` 可清除。"),
    "rejected": ("❌ Your dot rejected “{preview}” (HTTP {status}). `/resend` to retry, `/discard` to drop.",
                 "❌ dot 拒绝了消息「{preview}」（HTTP {status}）。`/resend` 重试，`/discard` 放弃。"),
    "file_too_big": ("❌ That file is over 20 MB and can't be sent to your dot.",
                     "❌ 文件超过 20MB，无法发给 dot。"),
    "upload_failed": ("❌ The image/file couldn't be uploaded to your dot (the WeCom link may have expired). "
                      "Please send it again.",
                      "❌ 图片/文件没能上传给 dot（WeCom 链接可能已过期）。请重新发送该文件。"),
    "uncertain": ("⚠️ Couldn't confirm that “{preview}” reached your dot, so it wasn't resent "
                  "automatically. If it's missing in ChatGPT, send `/resend`; `/pending` lists all.",
                  "⚠️ 无法确认消息「{preview}」是否已送达 dot，为避免重复没有自动重发。"
                  "如在 ChatGPT 中没看到，发送 `/resend`；`/pending` 查看全部。"),
    "dot_file_failed": ("📎 Your dot sent “{name}”, but it couldn't be forwarded. Open it in ChatGPT.",
                        "📎 dot 发送的文件「{name}」无法转发，请在 ChatGPT 中查看。"),
    "dot_file_too_big": ("📎 Your dot sent “{name}”, which is over 20 MB. Open it in ChatGPT.",
                         "📎 dot 发送的文件「{name}」超过 20MB，请在 ChatGPT 中查看。"),
    "attachment_notice": ("(Your dot sent an attachment; open it in ChatGPT.)",
                          "（dot 发送了附件，请在 ChatGPT 中查看）"),
    "unfinished_notice": ("\n\n(This dot message stayed unfinished for a long time; above is what it "
                          "said. Check ChatGPT.)",
                          "\n\n（这条 dot 消息长时间未完成，以上为当时内容，请在 ChatGPT 中查看）"),
    "recovered": ("⚠️ {n} message(s) were being sent when the bridge restarted and may not have reached "
                  "your dot. Send `/pending` to review.",
                  "⚠️ 服务重启时有 {n} 条消息正在发送，无法确认是否已送达 dot。发送 `/pending` 查看。"),
    "status_title": ("**dot bridge status**", "**dot 桥接状态**"),
    "status_wecom": ("- WeCom: {state}", "- WeCom：{state}"),
    "status_live": ("- dot live updates: {state}", "- dot 实时推送：{state}"),
    "status_chatgpt": ("- ChatGPT: {state}", "- ChatGPT：{state}"),
    "status_dot": ("- dot: {state}", "- dot：{state}"),
    "status_queue": ("- messages: {state}", "- 消息：{state}"),
    "status_backlog": ("- replies waiting: {n}", "- 待转发回复：{n}"),
    "status_pending": ("- some messages are unconfirmed/rejected: `/pending`",
                       "- 有未确认/被拒绝的消息：`/pending`"),
    "connected": ("connected", "已连接"),
    "disconnected": ("disconnected", "未连接"),
    "polling": ("disconnected (polling)", "未连接（轮询中）"),
    "signed_in": ("signed in", "已登录"),
    "signed_out": ("not signed in (`/login`)", "未登录（/login）"),
    "unbound": ("not connected", "未绑定"),
    "none": ("none", "无"),
}


def t(lang: str, key: str, **values) -> str:
    en, zh = M[key]
    return (zh if lang == "zh" else en).format(**values)
