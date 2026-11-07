use anyhow::{Context, Result, bail};
use eframe::egui;
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

const USER_AGENT: &str = "Mozilla/5.0";
const REALTIME_CONFIG_URL: &str = "https://web.kick.com/api/v1/realtime/connection";
const REALTIME_AUTH_URL: &str = "https://web.kick.com/api/v1/realtime/auth/connection";
const MAX_MESSAGES: usize = 5000;

#[derive(Clone)]
struct Badge {
    text: String,
    kind: String,
}

#[derive(Clone)]
struct ChatMsg {
    id: Option<String>,
    user: String,
    text: String,
    created_at: Option<String>,
    color: Option<String>,
    badges: Vec<Badge>,
    is_system: bool,
}

enum WorkerMsg {
    Status(String),
    Connected {
        broadcaster_user_id: u64,
    },
    Message(ChatMsg),
    Delete(String),
    System(String),
    Error(String),
}

struct KickterinoApp {
    channel: String,
    token: String,
    broadcaster_user_id: Option<u64>,
    messages: Vec<ChatMsg>,
    rx: Receiver<WorkerMsg>,
    tx: Sender<WorkerMsg>,
    input: String,
    status: String,
    generation: Arc<AtomicU64>,
    show_timestamps: bool,
    auto_scroll: bool,
}

impl KickterinoApp {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            channel: "xqc".to_string(),
            token: String::new(),
            broadcaster_user_id: None,
            messages: Vec::new(),
            rx,
            tx,
            input: String::new(),
            status: "Disconnected".to_string(),
            generation: Arc::new(AtomicU64::new(0)),
            show_timestamps: true,
            auto_scroll: true,
        }
    }

    fn connect(&mut self, ctx: egui::Context) {
        let channel = self.channel.trim().to_lowercase();
        if channel.is_empty() {
            return;
        }
        let tx = self.tx.clone();
        let generation = Arc::clone(&self.generation);
        let my_generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        self.status = format!("Connecting to {}...", channel);
        self.broadcaster_user_id = None;
        self.messages.clear();
        thread::spawn(move || {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    notify(&tx, &ctx, WorkerMsg::Error(format!("Runtime failed: {}", e)));
                    return;
                }
            };
            rt.block_on(async move {
                if let Err(e) = run_chat(&channel, &tx, &ctx, &generation, my_generation).await {
                    if generation.load(Ordering::SeqCst) == my_generation {
                        notify(&tx, &ctx, WorkerMsg::Error(format!("{:#}", e)));
                    }
                }
            });
        });
    }

    fn send_message(&mut self, ctx: egui::Context) {
        let text = self.input.trim().to_string();
        if text.is_empty() {
            return;
        }
        let token = self.token.trim().to_string();
        if token.is_empty() {
            self.status = "No access token set".to_string();
            return;
        }
        let uid = match self.broadcaster_user_id {
            Some(u) => u,
            None => {
                self.status = "Not connected".to_string();
                return;
            }
        };
        self.input.clear();
        let tx = self.tx.clone();
        thread::spawn(move || {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    notify(&tx, &ctx, WorkerMsg::Error(format!("Runtime failed: {}", e)));
                    return;
                }
            };
            rt.block_on(async move {
                let client = kick_api::KickApiClient::with_token(token);
                let req = kick_api::SendMessageRequest {
                    r#type: "user".to_string(),
                    content: text,
                    broadcaster_user_id: Some(uid),
                    reply_to_message_id: None,
                };
                match client.chat().send_message(req).await {
                    Ok(_) => notify(&tx, &ctx, WorkerMsg::Status("Message sent".to_string())),
                    Err(e) => notify(&tx, &ctx, WorkerMsg::Error(format!("Send failed: {}", e))),
                }
            });
        });
    }
}

impl eframe::App for KickterinoApp {
    fn logic(&mut self, _ctx: &egui::Context, _frame: &mut eframe::Frame) {
        while let Ok(m) = self.rx.try_recv() {
            match m {
                WorkerMsg::Status(s) => self.status = s,
                WorkerMsg::Connected { broadcaster_user_id } => {
                    self.broadcaster_user_id = Some(broadcaster_user_id);
                }
                WorkerMsg::Message(msg) => {
                    self.messages.push(msg);
                    if self.messages.len() > MAX_MESSAGES {
                        let drain = self.messages.len() - MAX_MESSAGES;
                        self.messages.drain(0..drain);
                    }
                }
                WorkerMsg::Delete(id) => {
                    self.messages.retain(|m| m.id.as_deref() != Some(id.as_str()));
                }
                WorkerMsg::System(text) => {
                    self.messages.push(ChatMsg {
                        id: None,
                        user: String::new(),
                        text,
                        created_at: None,
                        color: None,
                        badges: Vec::new(),
                        is_system: true,
                    });
                    if self.messages.len() > MAX_MESSAGES {
                        let drain = self.messages.len() - MAX_MESSAGES;
                        self.messages.drain(0..drain);
                    }
                }
                WorkerMsg::Error(e) => self.status = e,
            }
        }
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        egui::Panel::top("top").show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label("Channel:");
                ui.text_edit_singleline(&mut self.channel);
                if ui.button("Connect").clicked() {
                    self.connect(ui.ctx().clone());
                }
                if ui.button("Clear").clicked() {
                    self.messages.clear();
                }
                ui.checkbox(&mut self.show_timestamps, "Timestamps");
                ui.label(&self.status);
            });
            ui.horizontal(|ui| {
                ui.label("Access token:");
                ui.text_edit_singleline(&mut self.token);
            });
        });

        egui::Panel::bottom("bottom").show(ui, |ui| {
            ui.horizontal(|ui| {
                let response = ui.text_edit_singleline(&mut self.input);
                let send = ui.button("Send").clicked()
                    || (response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)));
                if send {
                    self.send_message(ui.ctx().clone());
                }
            });
        });

        egui::CentralPanel::default().show(ui, |ui| {
            egui::ScrollArea::vertical()
                .stick_to_bottom(self.auto_scroll)
                .show(ui, |ui| {
                    for m in &self.messages {
                        if m.is_system {
                            ui.label(
                                egui::RichText::new(&m.text)
                                    .italics()
                                    .color(egui::Color32::from_gray(160)),
                            );
                            continue;
                        }
                        ui.horizontal_wrapped(|ui| {
                            if self.show_timestamps {
                                if let Some(ts) = &m.created_at {
                                    ui.label(
                                        egui::RichText::new(ts)
                                            .small()
                                            .color(egui::Color32::from_gray(120)),
                                    );
                                }
                            }
                            for b in &m.badges {
                                let color = badge_color(&b.kind);
                                ui.label(
                                    egui::RichText::new(format!("[{}]", b.text))
                                        .small()
                                        .color(color),
                                );
                            }
                            let name_color = m
                                .color
                                .as_deref()
                                .and_then(parse_color)
                                .unwrap_or(egui::Color32::from_rgb(80, 160, 255));
                            ui.label(
                                egui::RichText::new(&m.user)
                                    .strong()
                                    .color(name_color),
                            );
                            ui.label(
                                egui::RichText::new(":").color(egui::Color32::from_gray(120)),
                            );
                            for (is_emote, seg) in split_content(&m.text) {
                                if is_emote {
                                    ui.label(
                                        egui::RichText::new(seg)
                                            .italics()
                                            .color(egui::Color32::from_rgb(255, 220, 120)),
                                    );
                                } else {
                                    ui.label(seg);
                                }
                            }
                        });
                    }
                });
        });
    }
}

fn notify(tx: &Sender<WorkerMsg>, ctx: &egui::Context, msg: WorkerMsg) {
    let _ = tx.send(msg);
    ctx.request_repaint();
}

fn parse_color(s: &str) -> Option<egui::Color32> {
    let s = s.trim().trim_start_matches('#');
    if s.len() == 6 {
        let r = u8::from_str_radix(&s[0..2], 16).ok()?;
        let g = u8::from_str_radix(&s[2..4], 16).ok()?;
        let b = u8::from_str_radix(&s[4..6], 16).ok()?;
        Some(egui::Color32::from_rgb(r, g, b))
    } else {
        None
    }
}

fn badge_color(kind: &str) -> egui::Color32 {
    match kind.to_lowercase().as_str() {
        "moderator" | "mod" => egui::Color32::from_rgb(0, 200, 80),
        "subscriber" | "sub" => egui::Color32::from_rgb(160, 100, 255),
        "vip" => egui::Color32::from_rgb(255, 100, 200),
        "broadcaster" | "owner" | "streamer" => egui::Color32::from_rgb(255, 60, 60),
        "sub_gifter" | "gifter" => egui::Color32::from_rgb(255, 165, 0),
        "verified" | "partner" => egui::Color32::from_rgb(0, 180, 255),
        _ => egui::Color32::from_gray(150),
    }
}

fn parse_badges(v: &Value) -> Vec<Badge> {
    let mut out = Vec::new();
    if let Some(arr) = v.as_array() {
        for b in arr {
            if let Some(s) = b.as_str() {
                out.push(Badge { text: s.to_string(), kind: s.to_lowercase() });
            } else if let Some(obj) = b.as_object() {
                let text = obj.get("text").and_then(|x| x.as_str()).unwrap_or("").to_string();
                let kind = obj.get("type").and_then(|x| x.as_str()).unwrap_or("").to_string();
                if !text.is_empty() {
                    out.push(Badge { text, kind });
                }
            }
        }
    }
    out
}

fn split_content(content: &str) -> Vec<(bool, String)> {
    let mut out = Vec::new();
    let mut rest = content;
    while let Some(start) = rest.find("[emote:") {
        if start > 0 {
            out.push((false, rest[..start].to_string()));
        }
        let after = &rest[start..];
        if let Some(end) = after.find(']') {
            let token = &after[..=end];
            let name = token.rsplit(':').next().unwrap_or("").trim_end_matches(']');
            out.push((true, name.to_string()));
            rest = &after[end + 1..];
        } else {
            out.push((false, rest.to_string()));
            rest = "";
            break;
        }
    }
    if !rest.is_empty() {
        out.push((false, rest.to_string()));
    }
    out
}

fn short_time(created_at: &str) -> Option<String> {
    if created_at.len() >= 19 {
        Some(created_at[11..19].to_string())
    } else {
        None
    }
}

struct ChannelInfo {
    broadcaster_user_id: u64,
    chatroom_id: u64,
}

#[derive(Deserialize)]
struct ChannelInfoResponse {
    #[serde(deserialize_with = "de_u64")]
    user_id: u64,
    chatroom: ChatroomResponse,
}

#[derive(Deserialize)]
struct ChatroomResponse {
    #[serde(deserialize_with = "de_u64")]
    id: u64,
}

fn de_u64<'de, D>(deserializer: D) -> std::result::Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Error;

    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(u64),
        Str(String),
    }

    match NumOrStr::deserialize(deserializer)? {
        NumOrStr::Num(n) => Ok(n),
        NumOrStr::Str(s) => s.trim().parse().map_err(D::Error::custom),
    }
}

async fn fetch_channel_info(slug: &str) -> Result<ChannelInfo> {
    let url = format!("https://kick.com/api/v2/channels/{}", slug);
    let mut cmd = tokio::process::Command::new("curl");
    cmd.args(["-sS", "-f", "-m", "20", "-H", "Accept: application/json", "-H", "User-Agent: Chatterino7", &url]);
    hide_console_window(&mut cmd);

    let output = cmd.output().await.context("failed to run curl (is it installed?)")?;
    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        bail!("Channel info failed for '{}': {}", slug, err.trim().trim_start_matches("curl: "));
    }

    let info: ChannelInfoResponse = serde_json::from_slice(&output.stdout)
        .with_context(|| format!("Channel info failed for '{}'", slug))?;
    Ok(ChannelInfo { broadcaster_user_id: info.user_id, chatroom_id: info.chatroom.id })
}

async fn post_json(url: &str, body: &Value) -> Result<Value> {
    let mut cmd = tokio::process::Command::new("curl");
    cmd.args(["-sS", "-f", "-m", "20", "-X", "POST", url, "-H", "Content-Type: application/json", "-H", "Accept: application/json", "-H", "x-app-platform: web", "-H", "Referer: https://kick.com/", "-H", "Origin: https://kick.com", "-H", &format!("User-Agent: {}", USER_AGENT), "-d", &body.to_string()]);
    hide_console_window(&mut cmd);

    let output = cmd.output().await.context("failed to run curl (is it installed?)")?;
    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        bail!("request failed: {}", err.trim().trim_start_matches("curl: "));
    }

    serde_json::from_slice(&output.stdout).context("invalid JSON response from Kick")
}

fn hide_console_window(cmd: &mut tokio::process::Command) {
    #[cfg(target_os = "windows")]
    {
        use std::os::windows::process::CommandExt;
        cmd.creation_flags(0x08000000);
    }
    #[cfg(not(target_os = "windows"))]
    {
        let _ = cmd;
    }
}

fn client_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or_default();
    format!("kickterino-{:x}-{:x}", std::process::id(), nanos)
}

async fn run_chat(channel: &str, tx: &Sender<WorkerMsg>, ctx: &egui::Context, generation: &Arc<AtomicU64>, my_generation: u64) -> Result<()> {
    let is_current = || generation.load(Ordering::SeqCst) == my_generation;

    let info = fetch_channel_info(channel).await?;
    if !is_current() {
        return Ok(());
    }
    notify(tx, ctx, WorkerMsg::Connected { broadcaster_user_id: info.broadcaster_user_id });

    let client = client_id();
    let config = post_json(REALTIME_CONFIG_URL, &json!({
        "client": { "id": client, "type": "web" },
        "capabilities": { "accepted_providers": [ { "provider": "pusher" }, { "provider": "centrifugo" } ] }
    })).await.context("Realtime config failed")?;

    let url = config["data"]["connections"][0]["credentials"]["url"].as_str().context("Realtime config returned no WebSocket URL")?.to_string();

    let auth = post_json(REALTIME_AUTH_URL, &json!({ "client_id": client })).await.context("Realtime auth failed")?;
    let token = auth["data"]["token"].as_str().context("Realtime auth returned no token")?.to_string();

    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.context("Chat WebSocket connection failed")?;

    let connect = json!({ "id": 1, "connect": { "token": token } });
    ws.send(Message::Text(connect.to_string().into())).await.context("Chat handshake failed")?;

    let reply = tokio::time::timeout(Duration::from_secs(15), ws.next()).await.context("Timed out while connecting to chat")?.context("Chat connection closed during handshake")?.context("Chat handshake failed")?;

    if let Message::Text(text) = &reply {
        let value: Value = serde_json::from_str(text.as_str()).context("Invalid chat handshake response")?;
        if let Some(err) = value.get("error") { bail!("Chat connection rejected: {}", err); }
        if value.get("connect").is_none() { bail!("Unexpected chat handshake response: {}", text.as_str()); }
    }

    let subscribe = json!({ "id": 2, "subscribe": { "channel": format!("chatrooms.{}.v2", info.chatroom_id) } });
    ws.send(Message::Text(subscribe.to_string().into())).await.context("Chat subscription failed")?;

    if is_current() {
        notify(tx, ctx, WorkerMsg::Status(format!("Connected to {}", channel)));
    }

    while let Some(frame) = ws.next().await {
        if !is_current() { break; }
        let frame = frame.context("Chat WebSocket error")?;
        match frame {
            Message::Text(text) => {
                let text = text.as_str();
                if text == "{}" {
                    ws.send(Message::Text("{}".into())).await.context("Chat pong failed")?;
                    continue;
                }
                let Ok(value) = serde_json::from_str::<Value>(text) else { continue; };
                if let Some(err) = value.get("error") { bail!("Chat error: {}", err); }
                if let Some(push) = value.get("push") { handle_push(push, tx, ctx); }
            }
            Message::Ping(payload) => { ws.send(Message::Pong(payload)).await.context("Chat pong failed")?; }
            Message::Close(_) => break,
            _ => {}
        }
    }

    if is_current() {
        notify(tx, ctx, WorkerMsg::Status("Connection closed".to_string()));
    }
    Ok(())
}

fn handle_push(push: &Value, tx: &Sender<WorkerMsg>, ctx: &egui::Context) {
    let data = &push["pub"]["data"];
    let event = data["event"].as_str().unwrap_or_default();

    let payload: Value = match &data["data"] {
        Value::String(raw) => match serde_json::from_str(raw) { Ok(value) => value, Err(_) => return },
        value @ Value::Object(_) => value.clone(),
        _ => return,
    };

    match event {
        "App\\Events\\ChatMessageEvent" => {
            let user = payload["sender"]["username"].as_str().unwrap_or_default().to_string();
            let text = payload["content"].as_str().unwrap_or_default().to_string();
            if user.is_empty() || text.is_empty() { return; }
            let id = payload["id"].as_str().map(|s| s.to_string()).or_else(|| payload["message_id"].as_str().map(|s| s.to_string()));
            let created_at = payload["created_at"].as_str().map(short_time).flatten();
            let identity = &payload["sender"]["identity"];
            let color = identity.get("color").or_else(|| identity.get("username_color")).and_then(|v| v.as_str()).map(|s| s.to_string());
            let badges = parse_badges(identity.get("badges").or_else(|| identity.get("badges_v2")).unwrap_or(&Value::Null));
            notify(tx, ctx, WorkerMsg::Message(ChatMsg { id, user, text, created_at, color, badges, is_system: false }));
        }
        "App\\Events\\MessageDeletedEvent" => {
            if let Some(id) = payload["message_id"].as_str() { notify(tx, ctx, WorkerMsg::Delete(id.to_string())); }
        }
        "App\\Events\\UserBannedEvent" => {
            let name = payload["username"].as_str().or_else(|| payload["banned_username"].as_str()).unwrap_or("someone");
            notify(tx, ctx, WorkerMsg::System(format!("{} was banned", name)));
        }
        "App\\Events\\UserUnbannedEvent" => {
            let name = payload["username"].as_str().or_else(|| payload["unbanned_username"].as_str()).unwrap_or("someone");
            notify(tx, ctx, WorkerMsg::System(format!("{} was unbanned", name)));
        }
        "App\\Events\\SubscriptionEvent" => {
            let name = payload["username"].as_str().or_else(|| payload["user"]["username"].as_str()).unwrap_or("someone");
            notify(tx, ctx, WorkerMsg::System(format!("{} subscribed", name)));
        }
        "App\\Events\\GiftedSubscriptionsEvent" => {
            let gifter = payload["gifted_by"].as_str().or_else(|| payload["gifter"]["username"].as_str()).unwrap_or("someone");
            let count = payload["recipients"].as_array().map(|a| a.len()).unwrap_or(0);
            notify(tx, ctx, WorkerMsg::System(format!("{} gifted {} subs", gifter, count)));
        }
        "App\\Events\\PinnedMessageCreatedEvent" => {
            if let Some(msg) = payload.get("message") {
                let user = msg["sender"]["username"].as_str().unwrap_or("someone");
                let content = msg["content"].as_str().unwrap_or("");
                notify(tx, ctx, WorkerMsg::System(format!("Pinned by {}: {}", user, content)));
            }
        }
        "App\\Events\\RewardRedeemedEvent" => {
            let user = payload["username"].as_str().unwrap_or("someone");
            let title = payload["reward_title"].as_str().unwrap_or("reward");
            notify(tx, ctx, WorkerMsg::System(format!("{} redeemed {}", user, title)));
        }
        "App\\Events\\KicksGiftedEvent" => {
            let sender = payload["sender"]["username"].as_str().unwrap_or("someone");
            let name = payload["gift"]["name"].as_str().unwrap_or("gift");
            let amount = payload["gift"]["amount"].as_i64().unwrap_or(1);
            notify(tx, ctx, WorkerMsg::System(format!("{} gifted {}x {}", sender, amount, name)));
        }
        _ => {}
    }
}

fn main() -> eframe::Result<()> {
    env_logger::init();
    let options = eframe::NativeOptions::default();
    eframe::run_native("kickterino", options, Box::new(|cc| Ok(Box::new(KickterinoApp::new(cc)))))
}
