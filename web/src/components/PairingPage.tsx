import { useCallback, useEffect, useState } from "react";
import QRCode from "qrcode";
import "./PairingPage.css";

/** Dedicated local pairing entry, also available after a browser has logged in. */
export function PairingPage() {
  const [svg, setSvg] = useState("");
  const [expires, setExpires] = useState(0);
  const [now, setNow] = useState(Date.now() / 1000);
  const [message, setMessage] = useState("正在连接电脑服务…");
  const [busy, setBusy] = useState(false);
  const [relay, setRelay] = useState("");
  const generate = useCallback(async () => {
    setBusy(true);
    setSvg("");
    try {
      const response = await fetch("/api/client-pairing", {
        method: "POST", credentials: "same-origin", cache: "no-store",
        headers: { "Content-Type": "application/json" }, body: "{}",
      });
      const body = await response.json();
      if (!response.ok || typeof body.payload !== "string") {
        setMessage(body.error === "wrapper_offline"
          ? "电脑连接服务尚未就绪，请在控制台点“启动 / 修复连接”，然后重试。"
          : "请在运行 CC Remote 的电脑上，用控制台的“显示配对二维码”按钮打开此页。");
        return;
      }
      setSvg(await QRCode.toString(body.payload, { type: "svg", errorCorrectionLevel: "M", margin: 4, width: 320 }));
      setRelay(JSON.parse(body.payload).relay);
      setExpires(body.expires_at);
      setNow(Date.now() / 1000);
      setMessage("打开手机 App，点“扫描配对二维码”或“重新扫码连接电脑”。");
    } catch {
      setMessage("未连接到电脑服务。请返回控制台点“排查故障”，服务恢复后重试。");
    } finally { setBusy(false); }
  }, []);
  useEffect(() => { void generate(); }, [generate]);
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now() / 1000), 1000);
    return () => window.clearInterval(timer);
  }, []);
  const remaining = Math.max(0, Math.ceil(expires - now));
  return <main className="pairing-screen">
    <section className="pairing-card">
      <p className="pairing-brand">CC Remote · 设备配对</p>
      <h1 style={{ fontSize: 26 }}>用手机扫码连接电脑</h1>
      <p style={{ lineHeight: 1.8 }}>{message}</p>
      {svg && remaining > 0 && <div className="pairing-qr" role="img" aria-label="CC Remote 配对二维码" dangerouslySetInnerHTML={{ __html: svg }} />}
      {svg && remaining === 0 && <p role="status">二维码已过期，请点下方刷新。</p>}
      {svg && <p className="pairing-address">{remaining > 0 ? `一次有效 · ${remaining} 秒后过期` : "已过期"}<br />{relay}</p>}
      <button className="pairing-refresh" onClick={() => void generate()} disabled={busy}>
        {busy ? "正在生成…" : svg ? "刷新二维码" : "重新生成二维码"}
      </button>
      <p style={{ fontSize: 14, lineHeight: 1.8 }}>手机与电脑连接同一 Wi-Fi。无需输入 IP 或密码。<br />断线、换网或配对过期后，都可以回到这里重新扫码。</p>
      <a href="/">打开网页版</a>
    </section>
  </main>;
}
