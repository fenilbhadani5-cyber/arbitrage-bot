use lazy_static::lazy_static;
use std::sync::Mutex;
use std::time::{Duration, Instant};

lazy_static! {
    static ref LAST_ALERT: Mutex<Option<Instant>> = Mutex::new(None);
}

pub async fn send_alert(msg: &str) {
    let mut last = LAST_ALERT.lock().unwrap();
    let now = Instant::now();
    if let Some(l) = *last {
        if now.duration_since(l) < Duration::from_secs(60) {
            // Throttle duplicate/spam alerts
            return;
        }
    }
    *last = Some(now);

    eprintln!("[ALERT] {}", msg);
    // In a real system with a webhook:
    // let client = Client::new();
    // let _ = client.post("https://hooks.slack.com/services/DUMMY").json(&serde_json::json!({"text": msg})).send().await;
}
