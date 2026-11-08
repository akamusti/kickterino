use anyhow::{Context, Result, bail};
use eframe::egui;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashSet;
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
    image_url: Option<String>,
}

#[derive(Clone)]
struct StreamInfo {
    title: String,
    category: String,
    viewers: String,
    followers: String,
    started_at: String,
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
    reply_to: Option<String>,
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
    StreamInfo(StreamInfo),
    Pinned(String),
    Unpinned,
}

#[derive(Serialize, Deserialize, Default)]
struct AppState {
    tabs: Vec<String>,
    active: usize,
    font_size: f32,
    timestamp_24h: bool,
    show_timestamps: bool,
    show_badges: bool,
    theme_dark: bool,
    always_on_top: bool,
}

struct ChatTab {
    channel: String,
    broadcaster_user_id: Option<u64>,
    messages: Vec<ChatMsg>,
    rx: Receiver<WorkerMsg>,
    tx: Sender<WorkerMsg>,
    input: String,
    status: String,
    generation: Arc<AtomicU64>,
    search_query: String,
    search_open: bool,
    ignored: HashSet<String>,
    seen: HashSet<String>,
    auto_scroll: bool,
    stream_info: Option<StreamInfo>,
    pinned: Option<String>,
}

impl ChatTab {
    fn new(channel: &str) -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            channel: channel.to_string(),
            broadcaster_user_id: None,
            messages: Vec::new(),
            rx,
            tx,
            input: String::new(),
            status: "Disconnected".to_string(),
            generation: Arc::new(AtomicU64::new(0)),
            search_query: String::new(),
            search_open: false,
            ignored: HashSet::new(),
            seen: HashSet::new(),
            auto_scroll: true,
            stream_info: None,
            pinned: None,
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
        self.seen.clear();
        thread::spawn(move || {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    notify(&tx, &ctx, WorkerMsg::Error(format!("Runtime failed: {}", e)));
                    return;
                }
            };
            rt.block_on(async move {
                loop {
                    if generation.load(Ordering::SeqCst) != my_generation {
                        break;
                    }
                    if let Err(e) = run_chat(&channel, &tx, &ctx, &generation, my_generation).await {
                        if generation.load(Ordering::SeqCst) == my_generation {
                            notify(&tx, &ctx, WorkerMsg::Error(format!("{:#}", e)));
                        }
                    }
                    if generation.load(Ordering::SeqCst) != my_generation {
                        break;
                    }
                    notify(&tx, &ctx, WorkerMsg::Status("Reconnecting in 3s...".to_string()));
                    tokio::time::sleep(Duration::from_secs(3)).await;
                }
            });
        });
    }

    fn send_message(&mut self, token: &str, ctx: egui::Context) {
        let text = self.input.trim().to_string();
        if text.is_empty() {
            return;
        }
        let token = token.trim().to_string();
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

enum EmoteState {
    Loading,
    Ready(Vec<u8>),
    Failed,
}

struct UserCardData {
    username: String,
    bio: String,
    avatar_url: String,
    followers: String,
}

struct UserCard {
    username: String,
    loading: bool,
    data: Option<UserCardData>,
    open: bool,
}

struct KickterinoApp {
    tabs: Vec<ChatTab>,
    active: usize,
    token: String,
    show_timestamps: bool,
    show_badges: bool,
    font_size: f32,
    timestamp_24h: bool,
    theme_dark: bool,
    always_on_top: bool,
    show_settings: bool,
    show_side_panel: bool,
    highlight_words: String,
    split_view: bool,
    active_right: usize,
    emote_cache: Arc<std::sync::Mutex<std::collections::HashMap<String, EmoteState>>>,
    emote_textures: std::collections::HashMap<String, egui::TextureHandle>,
    user_card: Option<UserCard>,
    state_path: String,
}

impl KickterinoApp {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        let state_path = format!(
            "{}/.config/kickterino/state.json",
            std::env::var("HOME").unwrap_or_else(|_| ".".to_string())
        );
        // ensure dir
        if let Some(parent) = std::path::Path::new(&state_path).parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let state: AppState = std::fs::read_to_string(&state_path)
            .ok()
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();

        let mut tabs = Vec::new();
        if state.tabs.is_empty() {
            tabs.push(ChatTab::new("xqc"));
        } else {
            for c in &state.tabs {
                tabs.push(ChatTab::new(c));
            }
        }
        let active = state.active.min(tabs.len().saturating_sub(1));

        Self {
            tabs,
            active,
            token: String::new(),
            show_timestamps: if state.show_timestamps { true } else { true },
            show_badges: if state.show_badges { true } else { true },
            font_size: if state.font_size > 0.0 { state.font_size } else { 14.0 },
            timestamp_24h: state.timestamp_24h || state.timestamp_24h == false && true,
            theme_dark: true,
            always_on_top: false,
            show_settings: false,
            show_side_panel: true,
            highlight_words: String::new(),
            split_view: false,
            active_right: 0,
            emote_cache: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            emote_textures: std::collections::HashMap::new(),
            user_card: None,
            state_path,
        }
    }

    fn save_state(&self) {
        let state = AppState {
            tabs: self.tabs.iter().map(|t| t.channel.clone()).collect(),
            active: self.active,
            font_size: self.font_size,
            timestamp_24h: self.timestamp_24h,
            show_timestamps: self.show_timestamps,
            show_badges: self.show_badges,
            theme_dark: self.theme_dark,
            always_on_top: self.always_on_top,
        };
        if let Ok(json) = serde_json::to_string_pretty(&state) {
            let _ = std::fs::write(&self.state_path, json);
        }
    }

    fn fetch_user_card(&mut self, username: String) {
        let url = format!("https://kick.com/api/v2/channels/{}", username.to_lowercase());
        let mut cmd = std::process::Command::new("curl");
        cmd.args(["-sS", "-f", "-m", "20", "-H", "Accept: application/json", "-H", "User-Agent: Mozilla/5.0", &url]);
        let output = cmd.output();
        let mut data = None;
        if let Ok(o) = output {
            if o.status.success() {
                if let Ok(v) = serde_json::from_slice::<Value>(&o.stdout) {
                    data = Some(UserCardData {
                        username: v["user"]["username"].as_str().unwrap_or(&username).to_string(),
                        bio: v["user"]["bio"].as_str().unwrap_or("").to_string(),
                        avatar_url: v["user"]["profile_pic"].as_str().unwrap_or("").to_string(),
                        followers: v["followers_count"].as_str().unwrap_or("0").to_string(),
                    });
                }
            }
        }
        self.user_card = Some(UserCard {
            username,
            loading: false,
            data,
            open: true,
        });
    }
}

impl eframe::App for KickterinoApp {
    fn logic(&mut self, _ctx: &egui::Context, _frame: &mut eframe::Frame) {
        for tab in &mut self.tabs {
            while let Ok(m) = tab.rx.try_recv() {
                match m {
                    WorkerMsg::Status(s) => tab.status = s,
                    WorkerMsg::Connected { broadcaster_user_id } => {
                        tab.broadcaster_user_id = Some(broadcaster_user_id);
                    }
                    WorkerMsg::Message(msg) => {
                        if let Some(id) = &msg.id {
                            if !tab.seen.insert(id.clone()) {
                                continue;
                            }
                        }
                        if !msg.is_system && tab.ignored.contains(&msg.user.to_lowercase()) {
                            continue;
                        }
                        tab.messages.push(msg);
                        if tab.messages.len() > MAX_MESSAGES {
                            let drain = tab.messages.len() - MAX_MESSAGES;
                            tab.messages.drain(0..drain);
                        }
                    }
                    WorkerMsg::Delete(id) => {
                        tab.messages.retain(|m| m.id.as_deref() != Some(id.as_str()));
                    }
                    WorkerMsg::System(text) => {
                        tab.messages.push(ChatMsg {
                            id: None,
                            user: String::new(),
                            text,
                            created_at: None,
                            color: None,
                            badges: Vec::new(),
                            is_system: true,
                            reply_to: None,
                        });
                        if tab.messages.len() > MAX_MESSAGES {
                            let drain = tab.messages.len() - MAX_MESSAGES;
                            tab.messages.drain(0..drain);
                        }
                    }
                    WorkerMsg::StreamInfo(si) => tab.stream_info = Some(si),
                    WorkerMsg::Pinned(p) => tab.pinned = Some(p),
                    WorkerMsg::Unpinned => tab.pinned = None,
                    WorkerMsg::Error(e) => tab.status = e,
                }
            }
        }
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        if self.theme_dark {
            ui.ctx().set_visuals(egui::Visuals::dark());
        } else {
            ui.ctx().set_visuals(egui::Visuals::light());
        }
        if self.always_on_top {
            ui.ctx().send_viewport_cmd(egui::ViewportCommand::WindowLevel(egui::WindowLevel::AlwaysOnTop));
        } else {
            ui.ctx().send_viewport_cmd(egui::ViewportCommand::WindowLevel(egui::WindowLevel::Normal));
        }
        // keyboard shortcuts for active tab
        let i = self.active;
        if ui.input(|inp| inp.modifiers.ctrl && inp.key_pressed(egui::Key::L)) {
            if let Some(t) = self.tabs.get_mut(i) {
                t.messages.clear();
                t.seen.clear();
            }
        }
        if ui.input(|inp| inp.modifiers.ctrl && inp.key_pressed(egui::Key::F)) {
            if let Some(t) = self.tabs.get_mut(i) {
                t.search_open = true;
            }
        }

        // top: tab bar (Chatterino-style: clean, compact)
        egui::Panel::top("top").show(ui, |ui| {
            ui.horizontal(|ui| {
                for idx in 0..self.tabs.len() {
                    let label = if self.tabs[idx].channel.is_empty() {
                        "new".to_string()
                    } else {
                        self.tabs[idx].channel.clone()
                    };
                    let selected = idx == self.active;
                    let tab_label = if selected {
                        egui::RichText::new(&label).strong()
                    } else {
                        egui::RichText::new(&label)
                    };
                    let tab_resp = ui.selectable_label(selected, tab_label);
                    if tab_resp.clicked() {
                        self.active = idx;
                    }
                    // Chatterino-style: close tab via middle click or right click
                    let close = self.tabs.len() > 1
                        && (tab_resp.middle_clicked() || tab_resp.secondary_clicked());
                    if close {
                        self.tabs.remove(idx);
                        if self.active >= self.tabs.len() {
                            self.active = self.tabs.len().saturating_sub(1);
                        }
                        return;
                    }
                }
                if ui.small_button("+").clicked() {
                    self.tabs.push(ChatTab::new(""));
                    self.active = self.tabs.len() - 1;
                }
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    if ui.small_button("⚙").clicked() {
                        self.show_settings = !self.show_settings;
                    }
                });
            });
        });

        // channel control bar
        if let Some(tab) = self.tabs.get_mut(self.active) {
            egui::Panel::top("chan").show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label("Channel:");
                    ui.add(egui::TextEdit::singleline(&mut tab.channel).desired_width(140.0));
                    let ctx = ui.ctx().clone();
                    if ui.button("Connect").clicked() {
                        tab.connect(ctx);
                    }
                    if ui.button("Clear").clicked() {
                        tab.messages.clear();
                        tab.seen.clear();
                    }
                    if ui.button(if tab.search_open { "Hide search" } else { "Search" }).clicked() {
                        tab.search_open = !tab.search_open;
                    }
                    ui.checkbox(&mut tab.auto_scroll, "Auto-scroll");
                    if ui.button("Jump bottom").clicked() {
                        tab.auto_scroll = true;
                    }
                    ui.label(&tab.status);
                });
                // compact stream info / pinned bar
                let has_info = tab.stream_info.is_some();
                let has_pinned = tab.pinned.is_some();
                if has_info || has_pinned {
                    ui.horizontal_wrapped(|ui| {
                        if let Some(si) = &tab.stream_info {
                            if !si.title.is_empty() {
                                ui.label(egui::RichText::new(&si.title).strong().small());
                            }
                            if !si.category.is_empty() {
                                ui.label(egui::RichText::new(format!("· {}", si.category)).small());
                            }
                            ui.label(egui::RichText::new(format!("· 👁 {}", si.viewers)).small());
                            ui.label(egui::RichText::new(format!("· ❤ {}", si.followers)).small());
                        }
                        if let Some(pinned) = &tab.pinned {
                            ui.label(egui::RichText::new(format!("📌 {}", pinned)).small().strong());
                        }
                    });
                }
                if tab.search_open {
                    ui.horizontal(|ui| {
                        ui.label("Search:");
                        ui.text_edit_singleline(&mut tab.search_query);
                        if ui.button("x").clicked() {
                            tab.search_query.clear();
                            tab.search_open = false;
                        }
                    });
                }
            });
        }

        // settings in a separate native window (Chatterino-style), not embedded in app
        if self.show_settings {
            let mut still_open = true;
            ui.ctx().show_viewport_immediate(
                egui::ViewportId::from_hash_of("settings"),
                egui::ViewportBuilder::default()
                    .with_title("Settings")
                    .with_inner_size([440.0, 560.0])
                    .with_resizable(true),
                |ctx, _class| {
                    if ctx.input(|i| i.viewport().close_requested()) {
                        still_open = false;
                    }
                    egui::CentralPanel::default().show(ctx, |ui| {
                        egui::ScrollArea::vertical().show(ui, |ui| {
                            egui::CollapsingHeader::new("Appearance").default_open(true).show(ui, |ui| {
                                ui.horizontal(|ui| {
                                    ui.label("Font size:");
                                    ui.add(egui::Slider::new(&mut self.font_size, 10.0..=22.0));
                                });
                                ui.checkbox(&mut self.theme_dark, "Dark theme");
                                ui.checkbox(&mut self.always_on_top, "Always on top");
                            });
                            egui::CollapsingHeader::new("Chat").default_open(true).show(ui, |ui| {
                                ui.checkbox(&mut self.show_timestamps, "Show timestamps");
                                ui.checkbox(&mut self.show_badges, "Show badges");
                                ui.checkbox(&mut self.timestamp_24h, "24h time");
                                ui.checkbox(&mut self.show_side_panel, "Show side panel");
                                ui.checkbox(&mut self.split_view, "Split view (2 columns)");
                                ui.label("Highlight words (comma separated):");
                                ui.text_edit_singleline(&mut self.highlight_words);
                                ui.label("Timestamps, badges and emotes are rendered inline.");
                            });
                            egui::CollapsingHeader::new("Behavior").default_open(false).show(ui, |ui| {
                                ui.label("Auto-scroll sticks to bottom while new messages arrive.");
                                if let Some(tab) = self.tabs.get_mut(self.active) {
                                    ui.checkbox(&mut tab.auto_scroll, "Auto-scroll (active tab)");
                                }
                            });
                            egui::CollapsingHeader::new("Account").default_open(false).show(ui, |ui| {
                                ui.label("Kick access token (for sending messages):");
                                ui.text_edit_singleline(&mut self.token);
                                ui.small("Token is stored in memory only.");
                            });
                        });
                    });
                },
            );
            if !still_open {
                self.show_settings = false;
            }
        }

        // bottom input
        egui::Panel::bottom("bottom").show(ui, |ui| {
            ui.horizontal(|ui| {
                let tab = &mut self.tabs[self.active];
                let response = ui.text_edit_singleline(&mut tab.input);
                let send = ui.button("Send").clicked()
                    || (response.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter)));
                if send {
                    let ctx = ui.ctx().clone();
                    let token = self.token.clone();
                    tab.send_message(&token, ctx);
                }
            });
        });

        // apply font
        ui.style_mut().text_styles.get_mut(&egui::TextStyle::Body).unwrap().size = self.font_size;

        // split view right column (read-only chat of selected tab)
        if self.split_view {
            egui::Panel::right("splitright").resizable(true).default_size(400.0).show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.label("Tab:");
                    // selector
                    egui::ComboBox::from_id_salt("split_tab")
                        .selected_text(if self.tabs.is_empty() { "none".to_string() } else { self.tabs[self.active_right.min(self.tabs.len()-1)].channel.clone() })
                        .show_ui(ui, |ui| {
                            for idx in 0..self.tabs.len() {
                                let lbl = if self.tabs[idx].channel.is_empty() { "new".to_string() } else { self.tabs[idx].channel.clone() };
                                if ui.selectable_label(self.active_right == idx, lbl).clicked() {
                                    self.active_right = idx;
                                }
                            }
                        });
                });
                ui.separator();
                let idx = self.active_right.min(self.tabs.len().saturating_sub(1));
                let msgs = self.tabs[idx].messages.clone();
                let auto = self.tabs[idx].auto_scroll;
                egui::ScrollArea::vertical().stick_to_bottom(auto).show(ui, |ui| {
                    for m in &msgs {
                        if m.is_system {
                            ui.label(egui::RichText::new(&m.text).italics().color(egui::Color32::from_gray(160)));
                            continue;
                        }
                        let row = ui.horizontal_wrapped(|ui| {
                            if self.show_timestamps {
                                if let Some(ts) = &m.created_at {
                                    ui.label(egui::RichText::new(ts).small().color(egui::Color32::from_gray(100)));
                                }
                            }
                            let name_color = m.color.as_deref().and_then(parse_color).unwrap_or(egui::Color32::from_rgb(80,160,255));
                            ui.label(egui::RichText::new(&m.user).strong().color(name_color));
                            ui.label(egui::RichText::new(":").color(egui::Color32::from_gray(120)));
                            for (is_emote, seg) in split_content(&m.text) {
                                if is_emote { render_emote(ui, self, &seg); } else { ui.label(seg); }
                            }
                        });
                        let _ = row;
                    }
                });
            });
        }

        // side panel (Chatterino-style channel info)
        if self.show_side_panel {
            egui::Panel::right("side").resizable(true).default_size(220.0).show(ui, |ui| {
                ui.heading("Channel");
                if let Some(tab) = self.tabs.get(self.active) {
                    ui.label(format!("Name: {}", if tab.channel.is_empty() { "—" } else { &tab.channel }));
                    ui.label(format!("Status: {}", tab.status));
                    if let Some(si) = &tab.stream_info {
                        ui.separator();
                        ui.label(egui::RichText::new("Stream info").strong());
                        if !si.title.is_empty() { ui.label(format!("Title: {}", si.title)); }
                        if !si.category.is_empty() { ui.label(format!("Category: {}", si.category)); }
                        ui.label(format!("Viewers: {}", si.viewers));
                        ui.label(format!("Followers: {}", si.followers));
                        if !si.started_at.is_empty() { ui.label(format!("Started: {}", si.started_at)); }
                    }
                    if let Some(pinned) = &tab.pinned {
                        ui.separator();
                        ui.label(egui::RichText::new("Pinned").strong());
                        ui.label(pinned);
                    }
                    ui.separator();
                    ui.label(format!("Messages: {}", tab.messages.len()));
                    ui.label(format!("Ignored users: {}", tab.ignored.len()));
                }
            });
        }

        egui::CentralPanel::default().show(ui, |ui| {
            ui.spacing_mut().item_spacing = egui::vec2(4.0, 2.0);
            let i = self.active;
            let auto_scroll = self.tabs[i].auto_scroll;
            let search_query = self.tabs[i].search_query.clone();
            let msgs: Vec<ChatMsg> = self.tabs[i].messages.clone();
            let mut pending_ignore: Vec<String> = Vec::new();
            let mut pending_user_card: Option<String> = None;
            let mut pending_reply: Option<String> = None;
            egui::ScrollArea::vertical()
                .stick_to_bottom(auto_scroll)
                .show(ui, |ui| {
                    let query = search_query.to_lowercase();
                    let mut last_user: Option<String> = None;
                    for m in &msgs {
                        if !query.is_empty() {
                            let hay = format!("{} {}", m.user, m.text).to_lowercase();
                            if !hay.contains(&query) {
                                continue;
                            }
                        }
                        if m.is_system {
                            ui.label(
                                egui::RichText::new(&m.text)
                                    .italics()
                                    .color(egui::Color32::from_gray(160)),
                            );
                            last_user = None;
                            continue;
                        }
                        let grouped = last_user.as_deref() == Some(m.user.as_str());
                        if let Some(rt) = &m.reply_to {
                            ui.label(
                                egui::RichText::new(format!("↪ {}", rt))
                                    .small()
                                    .italics()
                                    .color(egui::Color32::from_gray(140)),
                            );
                        }
                        let is_mention = m.text.contains('@');
                        let lower_text = m.text.to_lowercase();
                        let is_highlight = !self.highlight_words.trim().is_empty()
                            && self.highlight_words.split(',').any(|w| {
                                let w = w.trim().to_lowercase();
                                !w.is_empty() && lower_text.contains(&w)
                            });
                        let row = ui.horizontal_wrapped(|ui| {
                            if self.show_timestamps {
                                if let Some(ts) = &m.created_at {
                                    let t = if self.timestamp_24h {
                                        ts.clone()
                                    } else {
                                        to_12h(ts)
                                    };
                                    ui.label(
                                        egui::RichText::new(t)
                                            .small()
                                            .color(egui::Color32::from_gray(100)),
                                    );
                                }
                            }
                            if self.show_badges && !grouped {
                                for b in &m.badges {
                                    if let Some(url) = &b.image_url {
                                        render_badge_image(ui, self, url, &b.text);
                                    } else {
                                        let color = badge_color(&b.kind);
                                        ui.label(
                                            egui::RichText::new(format!("[{}]", b.text))
                                                .small()
                                                .color(color),
                                        );
                                    }
                                }
                            }
                            let name_color = m
                                .color
                                .as_deref()
                                .and_then(parse_color)
                                .unwrap_or(egui::Color32::from_rgb(80, 160, 255));
                            if !grouped {
                                let name_resp = ui.add(
                                    egui::Label::new(
                                        egui::RichText::new(&m.user).strong().color(name_color),
                                    )
                                    .sense(egui::Sense::click()),
                                );
                                if name_resp.clicked() {
                                    pending_user_card = Some(m.user.clone());
                                }
                                ui.label(
                                    egui::RichText::new(":").color(egui::Color32::from_gray(120)),
                                );
                            } else {
                                // grouped: indent to align with message text
                                ui.label(egui::RichText::new("   ").small().color(egui::Color32::TRANSPARENT));
                            }
                            let text_color = if is_mention {
                                egui::Color32::from_rgb(255, 230, 120)
                            } else {
                                egui::Color32::PLACEHOLDER
                            };
                            for (is_emote, seg) in split_content(&m.text) {
                                if is_emote {
                                    render_emote(ui, self, &seg);
                                } else if is_mention {
                                    ui.label(egui::RichText::new(seg).color(text_color));
                                } else {
                                    ui.label(seg);
                                }
                            }
                        });
                        // row background: mention / highlight / hover
                        let rect = row.response.rect;
                        if is_mention {
                            ui.painter().rect_filled(rect, 2.0, egui::Color32::from_rgba_unmultiplied(255, 230, 120, 18));
                        } else if is_highlight {
                            ui.painter().rect_filled(rect, 2.0, egui::Color32::from_rgba_unmultiplied(120, 200, 255, 18));
                        }
                        if row.response.hovered() {
                            ui.painter().rect_filled(rect, 2.0, egui::Color32::from_rgba_unmultiplied(255,255,255,12));
                        }
                        let msg = m.clone();
                        row.response.context_menu(|ui| {
                            if ui.button("Copy message").clicked() {
                                ui.ctx().copy_text(msg.text.clone());
                            }
                            if ui.button("Copy username").clicked() {
                                ui.ctx().copy_text(msg.user.clone());
                            }
                            if ui.button("Reply").clicked() {
                                pending_reply = Some(msg.user.clone());
                            }
                            if ui.button("Ignore user").clicked() {
                                pending_ignore.push(msg.user.to_lowercase());
                            }
                            if !self.token.is_empty() {
                                if let Some(id) = &msg.id {
                                    let idc = id.clone();
                                    let tok = self.token.clone();
                                    if ui.button("Delete message (mod)").clicked() {
                                        thread::spawn(move || {
                                            let rt = tokio::runtime::Runtime::new().unwrap();
                                            rt.block_on(async move {
                                                let client = kick_api::KickApiClient::with_token(tok);
                                                let _ = client.chat().delete_message(&idc).await;
                                            });
                                        });
                                    }
                                }
                            }
                        });
                        last_user = Some(m.user.clone());
                    }
                });
            // apply pending
            let i = self.active;
            for u in pending_ignore {
                self.tabs[i].ignored.insert(u);
            }
            if let Some(u) = pending_user_card {
                self.fetch_user_card(u);
            }
            if let Some(u) = pending_reply {
                let i = self.active;
                self.tabs[i].input = format!("@{} ", u);
            }
        });

        // user card window
        if let Some(uc) = &mut self.user_card {
            if uc.open {
                let mut open = uc.open;
                egui::Window::new("User card")
                    .open(&mut open)
                    .show(ui.ctx(), |ui| {
                        if let Some(d) = &uc.data {
                            ui.heading(&d.username);
                            ui.label(format!("Followers: {}", d.followers));
                            if !d.bio.is_empty() {
                                ui.label(&d.bio);
                            }
                        } else {
                            ui.label("No data");
                        }
                    });
                uc.open = open;
                if !uc.open {
                    self.user_card = None;
                }
            }
        }

        self.save_state();
    }
}

fn render_emote(ui: &mut egui::Ui, app: &mut KickterinoApp, id: &str) {
    // ensure fetch started
    {
        let mut cache = app.emote_cache.lock().unwrap();
        if !cache.contains_key(id) {
            cache.insert(id.to_string(), EmoteState::Loading);
            let id2 = id.to_string();
            let cache2 = Arc::clone(&app.emote_cache);
            let ctx2 = ui.ctx().clone();
            thread::spawn(move || {
                let url = format!("https://files.kick.com/emotes/{}/fullsize", id2);
                let mut cmd = std::process::Command::new("curl");
                cmd.args(["-sS", "-f", "-m", "20", "-H", "User-Agent: Mozilla/5.0", &url]);
                let res = cmd.output();
                let mut cache = cache2.lock().unwrap();
                match res {
                    Ok(o) if o.status.success() && !o.stdout.is_empty() => {
                        cache.insert(id2, EmoteState::Ready(o.stdout));
                    }
                    _ => {
                        cache.insert(id2, EmoteState::Failed);
                    }
                }
                ctx2.request_repaint();
            });
        }
    }

    if let Some(tex) = app.emote_textures.get(id) {
        ui.add(egui::Image::new(tex).max_size(egui::vec2(48.0, 48.0)));
        return;
    }

    let bytes_opt = {
        let cache = app.emote_cache.lock().unwrap();
        match cache.get(id) {
            Some(EmoteState::Ready(b)) => Some(b.clone()),
            _ => None,
        }
    };
    if let Some(bytes) = bytes_opt {
        // decode
        if let Ok(img) = image::load_from_memory(&bytes) {
            let rgba = img.to_rgba8();
            let size = [rgba.width() as usize, rgba.height() as usize];
            let color_image = egui::ColorImage::from_rgba_unmultiplied(size, rgba.as_flat_samples().as_slice());
            let tex = ui.ctx().load_texture(id, color_image, egui::TextureOptions::default());
            app.emote_textures.insert(id.to_string(), tex.clone());
            ui.add(egui::Image::new(&tex).max_size(egui::vec2(32.0, 32.0)));
            return;
        }
    }
    ui.label(format!("[{}]", id));
}


fn render_badge_image(ui: &mut egui::Ui, app: &mut KickterinoApp, url: &str, fallback: &str) {
    // ensure fetch
    {
        let mut cache = app.emote_cache.lock().unwrap();
        if !cache.contains_key(url) {
            cache.insert(url.to_string(), EmoteState::Loading);
            let url2 = url.to_string();
            let cache2 = std::sync::Arc::clone(&app.emote_cache);
            let ctx2 = ui.ctx().clone();
            thread::spawn(move || {
                let mut cmd = std::process::Command::new("curl");
                cmd.args(["-sS", "-f", "-m", "20", "-H", "User-Agent: Mozilla/5.0", &url2]);
                let res = cmd.output();
                let mut cache = cache2.lock().unwrap();
                match res {
                    Ok(o) if o.status.success() && !o.stdout.is_empty() => {
                        cache.insert(url2, EmoteState::Ready(o.stdout));
                    }
                    _ => {
                        cache.insert(url2, EmoteState::Failed);
                    }
                }
                ctx2.request_repaint();
            });
        }
    }

    if let Some(tex) = app.emote_textures.get(url) {
        ui.add(egui::Image::new(tex).max_size(egui::vec2(18.0, 18.0)));
        return;
    }
    let bytes_opt = {
        let cache = app.emote_cache.lock().unwrap();
        match cache.get(url) {
            Some(EmoteState::Ready(b)) => Some(b.clone()),
            _ => None,
        }
    };
    if let Some(bytes) = bytes_opt {
        if let Ok(img) = image::load_from_memory(&bytes) {
            let rgba = img.to_rgba8();
            let size = [rgba.width() as usize, rgba.height() as usize];
            let color_image = egui::ColorImage::from_rgba_unmultiplied(size, rgba.as_flat_samples().as_slice());
            let tex = ui.ctx().load_texture(url, color_image, egui::TextureOptions::default());
            app.emote_textures.insert(url.to_string(), tex.clone());
            ui.add(egui::Image::new(&tex).max_size(egui::vec2(18.0, 18.0)));
            return;
        }
    }
    let color = badge_color(fallback);
    ui.label(egui::RichText::new(format!("[{}]", fallback)).small().color(color));
}

fn to_12h(ts: &str) -> String {
    if ts.len() < 8 {
        return ts.to_string();
    }
    let h: u32 = ts[0..2].parse().unwrap_or(0);
    let rest = &ts[2..];
    let (h12, suffix) = if h == 0 {
        (12, "AM")
    } else if h < 12 {
        (h, "AM")
    } else if h == 12 {
        (12, "PM")
    } else {
        (h - 12, "PM")
    };
    format!("{}{} {}", h12, rest, suffix)
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
        "verified" | "partner" | "goldenk" => egui::Color32::from_rgb(0, 180, 255),
        "level" => egui::Color32::from_rgb(255, 200, 80),
        _ => egui::Color32::from_gray(150),
    }
}

fn parse_badges(v: &Value) -> Vec<Badge> {
    let mut out = Vec::new();
    if let Some(arr) = v.as_array() {
        for b in arr {
            if let Some(s) = b.as_str() {
                out.push(Badge { text: s.to_string(), kind: s.to_lowercase(), image_url: None });
            } else if let Some(obj) = b.as_object() {
                let text = obj
                    .get("text")
                    .or_else(|| obj.get("label"))
                    .or_else(|| obj.get("name"))
                    .or_else(|| obj.get("badge_type"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let kind = obj
                    .get("type")
                    .or_else(|| obj.get("badge_type"))
                    .or_else(|| obj.get("name"))
                    .and_then(|x| x.as_str())
                    .unwrap_or("")
                    .to_string();
                let image_url = obj
                    .get("image_url")
                    .or_else(|| obj.get("image"))
                    .and_then(|x| x.as_str())
                    .map(|s| s.to_string());
                if !text.is_empty() {
                    out.push(Badge { text, kind, image_url });
                } else if !kind.is_empty() {
                    out.push(Badge { text: kind.clone(), kind, image_url });
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
            // extract id: token format [emote:ID:NAME]
            let inner = token.trim_start_matches("[emote:").trim_end_matches(']');
            let id = inner.split(':').next().unwrap_or(name).to_string();
            // store id as seg for emote (we'll render by id); but name nicer? Use id for fetch.
            out.push((true, id));
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

fn parse_reply_to(m: &Value) -> Option<String> {
    // history: metadata JSON with original_message.sender.username
    if let Some(meta) = m.get("metadata") {
        let raw = meta.as_str().unwrap_or("");
        if !raw.is_empty() {
            if let Ok(v) = serde_json::from_str::<Value>(raw) {
                if let Some(om) = v.get("original_message") {
                    if let Some(u) = om["sender"]["username"].as_str() {
                        return Some(u.to_string());
                    }
                }
            }
        }
    }
    // realtime payload may include reply_to_message or replies_to
    if let Some(r) = m.get("reply_to") {
        if let Some(u) = r["sender"]["username"].as_str().or_else(|| r["username"].as_str()) {
            return Some(u.to_string());
        }
    }
    None
}

fn parse_history_msg(m: &Value) -> Option<ChatMsg> {
    let user = m["sender"]["username"].as_str().unwrap_or_default().to_string();
    let text = m["content"]
        .as_str()
        .or_else(|| m["text"].as_str())
        .unwrap_or_default()
        .to_string();
    if user.is_empty() || text.is_empty() {
        return None;
    }
    let id = m["id"].as_str().map(|s| s.to_string());
    let created_at = m["created_at"].as_str().map(short_time).flatten();
    let color = m["sender"]["color"]
        .as_str()
        .or_else(|| m["sender"]["identity"]["color"].as_str())
        .or_else(|| m["sender"]["identity"]["username_color"].as_str())
        .map(|s| s.to_string());
    let badges = parse_badges(
        m["sender"]
            .get("badges")
            .or_else(|| m["sender"]["identity"].get("badges"))
            .unwrap_or(&Value::Null),
    );
    let reply_to = parse_reply_to(m);
    Some(ChatMsg {
        id,
        user,
        text,
        created_at,
        color,
        badges,
        is_system: false,
        reply_to,
    })
}

async fn fetch_history(chatroom_id: u64) -> Vec<ChatMsg> {
    let url = format!("https://kick.com/api/v2/channels/{}/messages", chatroom_id);
    let mut cmd = tokio::process::Command::new("curl");
    cmd.args([
        "-sS",
        "-f",
        "-m",
        "20",
        "-H",
        "Accept: application/json",
        "-H",
        &format!("User-Agent: {}", USER_AGENT),
        &url,
    ]);
    hide_console_window(&mut cmd);
    let output = match cmd.output().await {
        Ok(o) => o,
        Err(_) => return Vec::new(),
    };
    if !output.status.success() {
        return Vec::new();
    }
    let value: Value = match serde_json::from_slice(&output.stdout) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };
    let mut msgs: Vec<ChatMsg> = value["data"]["messages"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(parse_history_msg)
        .collect();
    msgs.reverse();
    msgs
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
    cmd.args([
        "-sS",
        "-f",
        "-m",
        "20",
        "-H",
        "Accept: application/json",
        "-H",
        "User-Agent: Chatterino7",
        &url,
    ]);
    hide_console_window(&mut cmd);

    let output = cmd.output().await.context("failed to run curl (is it installed?)")?;
    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr);
        bail!(
            "Channel info failed for '{}': {}",
            slug,
            err.trim().trim_start_matches("curl: ")
        );
    }

    let info: ChannelInfoResponse = serde_json::from_slice(&output.stdout)
        .with_context(|| format!("Channel info failed for '{}'", slug))?;
    Ok(ChannelInfo {
        broadcaster_user_id: info.user_id,
        chatroom_id: info.chatroom.id,
    })
}


async fn fetch_stream_info(slug: &str) -> Option<StreamInfo> {
    let url = format!("https://kick.com/api/v2/channels/{}", slug);
    let mut cmd = tokio::process::Command::new("curl");
    cmd.args([
        "-sS",
        "-f",
        "-m",
        "20",
        "-H",
        "Accept: application/json",
        "-H",
        "User-Agent: Mozilla/5.0",
        &url,
    ]);
    hide_console_window(&mut cmd);
    let output = cmd.output().await.ok()?;
    if !output.status.success() {
        return None;
    }
    let v: Value = serde_json::from_slice(&output.stdout).ok()?;
    let user = &v["user"];
    let ls = &v["livestream"];
    let title = ls["session_title"].as_str().unwrap_or("").to_string();
    let category = ls["categories"]
        .as_array()
        .and_then(|a| a.first())
        .and_then(|c| c["name"].as_str())
        .unwrap_or("")
        .to_string();
    let viewers = ls["viewer_count"]
        .as_i64()
        .map(|n| n.to_string())
        .unwrap_or_else(|| ls["viewer_count"].as_str().unwrap_or("0").to_string());
    let followers = v["followers_count"].as_str().unwrap_or("0").to_string();
    let started = ls["started_at"].as_str().unwrap_or("").to_string();
    Some(StreamInfo {
        title,
        category,
        viewers,
        followers,
        started_at: started,
    })
}

async fn post_json(url: &str, body: &Value) -> Result<Value> {
    let mut cmd = tokio::process::Command::new("curl");
    cmd.args([
        "-sS",
        "-f",
        "-m",
        "20",
        "-X",
        "POST",
        url,
        "-H",
        "Content-Type: application/json",
        "-H",
        "Accept: application/json",
        "-H",
        "x-app-platform: web",
        "-H",
        "Referer: https://kick.com/",
        "-H",
        "Origin: https://kick.com",
        "-H",
        &format!("User-Agent: {}", USER_AGENT),
        "-d",
        &body.to_string(),
    ]);
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
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("kickterino-{:x}-{:x}", std::process::id(), nanos)
}

async fn run_chat(
    channel: &str,
    tx: &Sender<WorkerMsg>,
    ctx: &egui::Context,
    generation: &Arc<AtomicU64>,
    my_generation: u64,
) -> Result<()> {
    let is_current = || generation.load(Ordering::SeqCst) == my_generation;

    let info = fetch_channel_info(channel).await?;
    if !is_current() {
        return Ok(());
    }

    if let Some(si) = fetch_stream_info(channel).await {
        if is_current() {
            notify(tx, ctx, WorkerMsg::StreamInfo(si));
        }
    }

    let history = fetch_history(info.chatroom_id).await;
    for m in history {
        if is_current() {
            notify(tx, ctx, WorkerMsg::Message(m));
        }
    }

    notify(
        tx,
        ctx,
        WorkerMsg::Connected {
            broadcaster_user_id: info.broadcaster_user_id,
        },
    );

    let client = client_id();
    let config = post_json(
        REALTIME_CONFIG_URL,
        &json!({
            "client": { "id": client, "type": "web" },
            "capabilities": {
                "accepted_providers": [
                    { "provider": "pusher" },
                    { "provider": "centrifugo" }
                ]
            }
        }),
    )
    .await
    .context("Realtime config failed")?;

    let url = config["data"]["connections"][0]["credentials"]["url"]
        .as_str()
        .context("Realtime config returned no WebSocket URL")?
        .to_string();

    let auth = post_json(REALTIME_AUTH_URL, &json!({ "client_id": client }))
        .await
        .context("Realtime auth failed")?;
    let token = auth["data"]["token"]
        .as_str()
        .context("Realtime auth returned no token")?
        .to_string();

    let (mut ws, _) = tokio_tungstenite::connect_async(&url)
        .await
        .context("Chat WebSocket connection failed")?;

    let connect = json!({ "id": 1, "connect": { "token": token } });
    ws.send(Message::Text(connect.to_string().into()))
        .await
        .context("Chat handshake failed")?;

    let reply = tokio::time::timeout(Duration::from_secs(15), ws.next())
        .await
        .context("Timed out while connecting to chat")?
        .context("Chat connection closed during handshake")?
        .context("Chat handshake failed")?;

    if let Message::Text(text) = &reply {
        let value: Value =
            serde_json::from_str(text.as_str()).context("Invalid chat handshake response")?;
        if let Some(err) = value.get("error") {
            bail!("Chat connection rejected: {}", err);
        }
        if value.get("connect").is_none() {
            bail!("Unexpected chat handshake response: {}", text.as_str());
        }
    }

    let subscribe = json!({
        "id": 2,
        "subscribe": { "channel": format!("chatrooms.{}.v2", info.chatroom_id) }
    });
    ws.send(Message::Text(subscribe.to_string().into()))
        .await
        .context("Chat subscription failed")?;

    if is_current() {
        notify(
            tx,
            ctx,
            WorkerMsg::Status(format!("Connected to {}", channel)),
        );
    }

    while let Some(frame) = ws.next().await {
        if !is_current() {
            break;
        }
        let frame = frame.context("Chat WebSocket error")?;
        match frame {
            Message::Text(text) => {
                let text = text.as_str();
                if text == "{}" {
                    ws.send(Message::Text("{}".into()))
                        .await
                        .context("Chat pong failed")?;
                    continue;
                }
                let Ok(value) = serde_json::from_str::<Value>(text) else {
                    continue;
                };
                if let Some(err) = value.get("error") {
                    bail!("Chat error: {}", err);
                }
                if let Some(push) = value.get("push") {
                    handle_push(push, tx, ctx);
                }
            }
            Message::Ping(payload) => {
                ws.send(Message::Pong(payload))
                    .await
                    .context("Chat pong failed")?;
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    if is_current() {
        notify(
            tx,
            ctx,
            WorkerMsg::Status("Connection closed".to_string()),
        );
    }
    Ok(())
}

fn handle_push(push: &Value, tx: &Sender<WorkerMsg>, ctx: &egui::Context) {
    let data = &push["pub"]["data"];
    let event = data["event"].as_str().unwrap_or_default();

    let payload: Value = match &data["data"] {
        Value::String(raw) => match serde_json::from_str(raw) {
            Ok(value) => value,
            Err(_) => return,
        },
        value @ Value::Object(_) => value.clone(),
        _ => return,
    };

    match event {
        "App\\Events\\ChatMessageEvent" => {
            let user = payload["sender"]["username"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            let text = payload["content"].as_str().unwrap_or_default().to_string();
            if user.is_empty() || text.is_empty() {
                return;
            }
            let id = payload["id"]
                .as_str()
                .map(|s| s.to_string())
                .or_else(|| payload["message_id"].as_str().map(|s| s.to_string()));
            let created_at = payload["created_at"].as_str().map(short_time).flatten();
            let identity = &payload["sender"]["identity"];
            let color = identity
                .get("color")
                .or_else(|| identity.get("username_color"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let badges = parse_badges(
                identity
                    .get("badges")
                    .or_else(|| identity.get("badges_v2"))
                    .unwrap_or(&Value::Null),
            );
            let reply_to = parse_reply_to(&payload);
            notify(
                tx,
                ctx,
                WorkerMsg::Message(ChatMsg {
                    id,
                    user,
                    text,
                    created_at,
                    color,
                    badges,
                    is_system: false,
                    reply_to,
                }),
            );
        }
        "App\\Events\\MessageDeletedEvent" => {
            if let Some(id) = payload["message_id"].as_str() {
                notify(tx, ctx, WorkerMsg::Delete(id.to_string()));
            }
        }
        "App\\Events\\UserBannedEvent" => {
            let name = payload["username"]
                .as_str()
                .or_else(|| payload["banned_username"].as_str())
                .unwrap_or("someone");
            notify(tx, ctx, WorkerMsg::System(format!("{} was banned", name)));
        }
        "App\\Events\\UserUnbannedEvent" => {
            let name = payload["username"]
                .as_str()
                .or_else(|| payload["unbanned_username"].as_str())
                .unwrap_or("someone");
            notify(tx, ctx, WorkerMsg::System(format!("{} was unbanned", name)));
        }
        "App\\Events\\SubscriptionEvent" => {
            let name = payload["username"]
                .as_str()
                .or_else(|| payload["user"]["username"].as_str())
                .unwrap_or("someone");
            notify(tx, ctx, WorkerMsg::System(format!("{} subscribed", name)));
        }
        "App\\Events\\GiftedSubscriptionsEvent" => {
            let gifter = payload["gifted_by"]
                .as_str()
                .or_else(|| payload["gifter"]["username"].as_str())
                .unwrap_or("someone");
            let count = payload["recipients"]
                .as_array()
                .map(|a| a.len())
                .unwrap_or(0);
            notify(
                tx,
                ctx,
                WorkerMsg::System(format!("{} gifted {} subs", gifter, count)),
            );
        }
        "App\\Events\\PinnedMessageCreatedEvent" => {
            if let Some(msg) = payload.get("message") {
                let user = msg["sender"]["username"].as_str().unwrap_or("someone");
                let content = msg["content"].as_str().unwrap_or("");
                notify(
                    tx,
                    ctx,
                    WorkerMsg::System(format!("Pinned by {}: {}", user, content)),
                );
                notify(tx, ctx, WorkerMsg::Pinned(format!("{}: {}", user, content)));
            }
        }
        "App\\Events\\PinnedMessageDeletedEvent" => {
            notify(tx, ctx, WorkerMsg::Unpinned);
        }
        "App\\Events\\RewardRedeemedEvent" => {
            let user = payload["username"].as_str().unwrap_or("someone");
            let title = payload["reward_title"].as_str().unwrap_or("reward");
            notify(
                tx,
                ctx,
                WorkerMsg::System(format!("{} redeemed {}", user, title)),
            );
        }
        "App\\Events\\KicksGiftedEvent" => {
            let sender = payload["sender"]["username"].as_str().unwrap_or("someone");
            let name = payload["gift"]["name"].as_str().unwrap_or("gift");
            let amount = payload["gift"]["amount"].as_i64().unwrap_or(1);
            notify(
                tx,
                ctx,
                WorkerMsg::System(format!("{} gifted {}x {}", sender, amount, name)),
            );
        }
        "App\\Events\\PollCreatedEvent" | "App\\Events\\PollUpdatedEvent" => {
            let title = payload["title"].as_str().or_else(|| payload["poll"]["title"].as_str()).unwrap_or("Poll");
            notify(tx, ctx, WorkerMsg::System(format!("Poll: {}", title)));
        }
        "App\\Events\\RaidEvent" | "App\\Events\\RaidedEvent" => {
            let from = payload["raider"]["username"].as_str().or_else(|| payload["username"].as_str()).unwrap_or("someone");
            let viewers = payload["viewers"].as_i64().unwrap_or(0);
            notify(tx, ctx, WorkerMsg::System(format!("{} raided with {} viewers", from, viewers)));
        }
        "App\\Events\\HostEvent" | "App\\Events\\HostedEvent" => {
            let from = payload["hoster"]["username"].as_str().or_else(|| payload["username"].as_str()).unwrap_or("someone");
            notify(tx, ctx, WorkerMsg::System(format!("{} is hosting", from)));
        }
        _ => {}
    }
}

impl Drop for KickterinoApp {
    fn drop(&mut self) {
        self.save_state();
    }
}

fn main() -> eframe::Result<()> {
    env_logger::init();
    let mut options = eframe::NativeOptions::default();
    // window icon from bundled logo
    if let Ok(img) = image::load_from_memory(include_bytes!("../assets/logo.jpg")) {
        let rgba = img.to_rgba8();
        let (w, h) = rgba.dimensions();
        options.viewport.icon = Some(std::sync::Arc::new(egui::IconData {
            rgba: rgba.into_raw(),
            width: w,
            height: h,
        }));
    }
    eframe::run_native(
        "kickterino",
        options,
        Box::new(|cc| Ok(Box::new(KickterinoApp::new(cc)))),
    )
}

