"""
setup_notification.py — 一键配置可转债每日信号邮件推送

完成后:
  - 邮件配置保存到 signal_config.json
  - 发送一封测试邮件验证配置
  - 在 Windows 任务计划程序中注册每日 15:30 自动任务
"""

import io
import json
import os
import smtplib
import subprocess
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# ── 确保 stdout 支持中文 ──────────────────────────────────────
if hasattr(sys.stdout, "buffer") and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(DIR, "signal_config.json")

# ── 预设邮件服务商 ────────────────────────────────────────────
PROVIDERS = [
    ("Gmail",          "smtp.gmail.com",      587, False,
     "https://myaccount.google.com/apppasswords  (需开启两步验证)"),
    ("QQ邮箱",         "smtp.qq.com",         587, False,
     "QQ邮箱 -> 设置 -> 账户 -> SMTP服务 -> 生成授权码"),
    ("163邮箱",        "smtp.163.com",        465, True,
     "163邮箱 -> 设置 -> POP3/SMTP -> 开启SMTP -> 获取授权码"),
    ("Outlook/Hotmail","smtp.office365.com",  587, False,
     "登录密码即可，或前往 account.microsoft.com 生成应用密码"),
    ("自定义",         "",                    587, False, ""),
]


# ─────────────────────────────────────────────────────────────
# 步骤 1: 选择邮件服务商
# ─────────────────────────────────────────────────────────────
def step1_choose_provider() -> tuple:
    print("\n" + "=" * 55)
    print("  步骤 1/4  选择邮件服务商")
    print("=" * 55)
    print()
    for i, (name, server, port, ssl, _) in enumerate(PROVIDERS, 1):
        ssl_tag = "SSL" if ssl else "STARTTLS"
        if server:
            print(f"  {i}. {name:<16} {server}:{port} ({ssl_tag})")
        else:
            print(f"  {i}. {name}")
    print()

    while True:
        choice = input("  请输入序号 (1-5): ").strip()
        if choice.isdigit() and 1 <= int(choice) <= len(PROVIDERS):
            idx = int(choice) - 1
            name, server, port, use_ssl, hint = PROVIDERS[idx]
            if name == "自定义":
                server  = input("  SMTP 服务器地址: ").strip()
                port_s  = input("  端口 (默认 587): ").strip()
                port    = int(port_s) if port_s.isdigit() else 587
                ssl_s   = input("  使用 SSL (端口465)? (y/N): ").strip().lower()
                use_ssl = ssl_s == "y"
            print(f"\n  已选: {name}  ({server}:{port})")
            return server, port, use_ssl, hint
        print("  [!] 请输入 1-5 之间的数字。")


# ─────────────────────────────────────────────────────────────
# 步骤 2: 输入发件账号和授权码
# ─────────────────────────────────────────────────────────────
def step2_get_credentials(hint: str) -> tuple:
    print("\n" + "=" * 55)
    print("  步骤 2/4  输入发件账号 & 授权码")
    print("=" * 55)
    if hint:
        print(f"\n  授权码获取方式:")
        print(f"    {hint}")
    print("  注意: 填写授权码，不是登录密码！\n")

    sender   = input("  发件邮箱地址: ").strip()
    password = input("  授权码: ").strip()
    return sender, password


# ─────────────────────────────────────────────────────────────
# 步骤 3: 输入收件人
# ─────────────────────────────────────────────────────────────
def step3_get_recipients(sender: str) -> list:
    print("\n" + "=" * 55)
    print("  步骤 3/4  输入收件人")
    print("=" * 55)
    print(f"\n  直接回车 = 只发给自己 ({sender})")
    raw = input("  收件人邮箱 (多个用逗号分隔): ").strip()
    if not raw:
        return [sender]
    return [r.strip() for r in raw.split(",") if r.strip()]


# ─────────────────────────────────────────────────────────────
# 步骤 4: 测试推送并保存配置
# ─────────────────────────────────────────────────────────────
def _smtp_send(cfg: dict, subject: str, body: str) -> None:
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = cfg["sender_email"]
    msg["To"]      = ", ".join(cfg["recipient_emails"])
    msg.attach(MIMEText(body, "plain", "utf-8"))

    if cfg.get("use_ssl"):
        ctx = smtplib.SMTP_SSL(cfg["smtp_server"], cfg["smtp_port"])
    else:
        ctx = smtplib.SMTP(cfg["smtp_server"], cfg["smtp_port"])
        ctx.starttls()
    with ctx as s:
        s.login(cfg["sender_email"], cfg["sender_password"])
        s.sendmail(cfg["sender_email"], cfg["recipient_emails"], msg.as_string())


def step4_test_and_save(cfg: dict):
    print("\n" + "=" * 55)
    print("  步骤 4/4  测试推送 & 保存配置")
    print("=" * 55)

    body = (
        "配置成功！\n\n"
        "可转债每日信号已配置完毕，将在每个交易日 15:30 自动推送 Top 5 择券结果。\n\n"
        "过滤条件:\n"
        "  - 剩余期限 > 0.5 年\n"
        "  - 转股溢价率 < 30%\n"
        "  - 正股总市值 > 50 亿\n"
        "  - 信用评级 >= AA-"
    )
    try:
        _smtp_send(cfg, "[测试] 可转债信号推送配置成功", body)
        print("  [OK] 测试邮件已发送，请查收！")
    except Exception as e:
        print(f"  [!] 发送失败: {e}")
        print("  请检查邮箱地址和授权码是否正确。")
        retry = input("  是否重新配置? (y/N): ").strip().lower()
        if retry == "y":
            return None
        print("  跳过测试，继续保存配置。")

    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(cfg, f, ensure_ascii=False, indent=2)
    print(f"  配置已保存: {CONFIG_PATH}")
    return cfg


# ─────────────────────────────────────────────────────────────
# 注册 Windows 任务计划
# ─────────────────────────────────────────────────────────────
def register_task() -> None:
    print("\n" + "=" * 55)
    print("  注册 Windows 任务计划程序")
    print("=" * 55)

    bat_path  = os.path.join(DIR, "run_daily.bat")
    task_name = "转债每日信号"

    cmd = [
        "schtasks", "/create",
        "/tn", task_name,
        "/tr", f'"{bat_path}"',
        "/sc", "WEEKLY",
        "/d", "MON,TUE,WED,THU,FRI",
        "/st", "15:30",
        "/f",
        "/rl", "HIGHEST",
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, encoding="gbk")
    if result.returncode == 0:
        print(f'  [OK] 任务 "{task_name}" 已注册，每个交易日 15:30 自动运行。')
    else:
        cmd_noadmin = [c for c in cmd if c not in ("/rl", "HIGHEST")]
        result2 = subprocess.run(cmd_noadmin, capture_output=True, text=True, encoding="gbk")
        if result2.returncode == 0:
            print(f'  [OK] 任务 "{task_name}" 已注册（标准权限）。')
        else:
            print(f"  [!] 任务注册失败: {result.stderr.strip()}")
            print(f'  手动注册: schtasks /create /tn "{task_name}" /tr "{bat_path}"'
                  " /sc WEEKLY /d MON,TUE,WED,THU,FRI /st 15:30 /f")

    verify = subprocess.run(
        ["schtasks", "/query", "/tn", task_name, "/fo", "LIST"],
        capture_output=True, text=True, encoding="gbk"
    )
    if verify.returncode == 0:
        for line in verify.stdout.splitlines():
            if any(k in line for k in ["任务名", "下次运行", "状态", "Task", "Next Run", "Status"]):
                print(f"    {line.strip()}")


# ─────────────────────────────────────────────────────────────
# 主流程
# ─────────────────────────────────────────────────────────────
def main() -> None:
    print()
    print("╔═══════════════════════════════════════════════════╗")
    print("║     可转债每日择券信号 — 邮件推送配置向导          ║")
    print("╚═══════════════════════════════════════════════════╝")
    print()
    print("  完成后每个交易日 15:30 自动推送 Top 5 到您的邮箱。")
    print("  使用 Python 内置 smtplib，无需安装额外依赖。")

    server, port, use_ssl, hint = step1_choose_provider()
    sender, password            = step2_get_credentials(hint)
    recipients                  = step3_get_recipients(sender)

    cfg = {
        "provider":         "smtp",
        "smtp_server":      server,
        "smtp_port":        port,
        "use_ssl":          use_ssl,
        "sender_email":     sender,
        "sender_password":  password,
        "recipient_emails": recipients,
    }

    result = step4_test_and_save(cfg)
    if result is None:
        print("\n  配置未完成，请重新运行脚本。")
        return

    register_task()

    print()
    print("╔═══════════════════════════════════════════════════╗")
    print("║  安装完成！                                        ║")
    print("║                                                   ║")
    print(f"║  发件人: {sender:<40}║")
    print("║  每日推送: 周一至周五 15:30                        ║")
    print("║  手动运行: python daily_signal.py                  ║")
    print("╚═══════════════════════════════════════════════════╝")
    print()


if __name__ == "__main__":
    main()
