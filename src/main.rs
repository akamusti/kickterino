use eframe::egui;
use std::sync::mpsc::{self, Receiver, Sender};
use std::thread;

#[derive(Clone)]
struct ChatMsg {
    user: String,
    text: String,
}

enum WorkerMsg {
    Status(String),
    Message(ChatMsg),
    Error(String),
}

struct KickterinoApp {
    channel: String,
    messages: Vec<ChatMsg>,
    rx: Receiver<WorkerMsg>,
    tx: Sender<WorkerMsg>,
    input: String,
    status: String,
}

impl KickterinoApp {
    fn new(_cc: &eframe::CreationContext<'_>) -> Self {
        let (tx, rx) = mpsc::channel();
        Self {
            channel: "xqc".to_string(),
            messages: Vec::new(),
            rx,
            tx,
            input: String::new(),
            status: "Disconnected".to_string(),
        }
    }

    fn connect(&mut self) {
        let channel = self.channel.trim().to_string();
        if channel.is_empty() {
            return;
        }
        let tx = self.tx.clone();
        self.status = format!("Connecting to {}…", channel);
        self.messages.clear();
        thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async move {
                match kick_api::LiveChatClient::connect_by_username(&channel).await {
                    Ok(mut client) => {
                        let _ = tx.send(WorkerMsg::Status(format!("Connected to {}", channel)));
                        loop {
                            match client.next_message().await {
                                Ok(Some(m)) => {
                                    let msg = ChatMsg {
                                        user: m.sender.username,
                                        text: m.content,
                                    };
                                    if tx.send(WorkerMsg::Message(msg)).is_err() {
                                        break;
                                    }
                                }
                                Ok(None) => {
                                    let _ = tx.send(WorkerMsg::Status("Connection closed".to_string()));
                                    break;
                                }
                                Err(e) => {
                                    let _ = tx.send(WorkerMsg::Error(format!("{}", e)));
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let _ = tx.send(WorkerMsg::Error(format!("Connect failed: {}", e)));
                    }
                }
            });
        });
    }
}

impl eframe::App for KickterinoApp {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        while let Ok(m) = self.rx.try_recv() {
            match m {
                WorkerMsg::Status(s) => self.status = s,
                WorkerMsg::Message(msg) => self.messages.push(msg),
                WorkerMsg::Error(e) => self.status = e,
            }
        }
        ctx.request_repaint();
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        egui::Panel::top("top").show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.label("Channel:");
                ui.text_edit_singleline(&mut self.channel);
                if ui.button("Connect").clicked() {
                    self.connect();
                }
                ui.label(&self.status);
            });
        });

        egui::Panel::bottom("bottom").show(ui, |ui| {
            ui.horizontal(|ui| {
                ui.text_edit_singleline(&mut self.input);
                if ui.button("Send").clicked() {
                    self.input.clear();
                }
            });
        });

        egui::CentralPanel::default().show(ui, |ui| {
            egui::ScrollArea::vertical().stick_to_bottom(true).show(ui, |ui| {
                for m in &self.messages {
                    ui.horizontal_wrapped(|ui| {
                        ui.strong(&m.user);
                        ui.label(":");
                        ui.label(&m.text);
                    });
                }
            });
        });
    }
}

fn main() -> eframe::Result<()> {
    env_logger::init();
    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "kickterino",
        options,
        Box::new(|cc| Ok(Box::new(KickterinoApp::new(cc)))),
    )
}

