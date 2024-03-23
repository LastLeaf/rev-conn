use log::{error, warn, info, debug};
use clap::Parser;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::time::Instant;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use std::time::Duration;

mod proto;
use proto::*;

/// Start rev-conn server
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct AppConfig {
    /// The address and port to bind
    #[clap(short, long)]
    addr: String,
    /// The passcode
    #[clap(short = 'c', long)]
    passcode: String,
}

struct Services {
    map: HashMap<String, mpsc::Sender<LinkOp>>,
    link_src: HashMap<LinkId, mpsc::Sender<LinkOp>>,
    udp_link_src_target: HashMap<LinkId, (mpsc::Sender<LinkOp>, mpsc::Sender<LinkOp>, Instant)>,
}

struct ProvideServices {
    services: Arc<Mutex<Services>>,
    names: Vec<String>,
}

impl ProvideServices {
    fn new(
        services: Arc<Mutex<Services>>,
        provide: &Vec<String>,
        conn_send: &mpsc::Sender<LinkOp>,
        peer_addr: std::net::SocketAddr,
    ) -> Self {
        let mut names = Vec::with_capacity(provide.len());
        {
            let mut services = services.lock().unwrap();
            for name in provide.iter() {
                if let Some(ori) = services.map.insert(name.into(), conn_send.clone()) {
                    services.map.insert(name.into(), ori);
                    error!("Service {:?} has multiple clients registered (the client from {} is ignored)", name, peer_addr);
                } else {
                    info!("Service {:?} registered from {}", name, peer_addr);
                    names.push(name.to_string());
                }
            }
        }
        Self {
            services,
            names,
        }
    }
}

impl Drop for ProvideServices {
    fn drop(&mut self) {
        let mut services = self.services.lock().unwrap();
        for name in self.names.iter() {
            services.map.remove(name);
        }
    }
}

async fn connection(
    passcode: String,
    services: Arc<Mutex<Services>>,
    stream: TcpStream,
) -> Result<(), ConnectionError> {
    let peer_addr = stream.peer_addr()?;
    let (mut read_stream, mut write_stream) = stream.into_split();
    let info = match read_message::<ClientStartInfo>(&mut read_stream, 256).await? {
        Some(x) => x,
        None => {
            error!("A connection sent a wrong greet message");
            return Ok(());
        }
    };
    if info.version != PROTO_VERSION {
        error!("A connection sent a wrong proto version");
        return Ok(());
    }
    if info.passcode != passcode {
        error!("A connection sent a wrong passcode");
        return Ok(());
    }
    match &info.kind {
        ClientConnKind::Control { provide } => {
            info!("Control connection built from {}", peer_addr);
            let (conn_send, mut conn_recv) = mpsc::channel(65536);
            tokio::task::spawn(async move {
                loop {
                    match tokio::time::timeout(std::time::Duration::from_secs(KEEP_ALIVE_INTERVAL as u64), conn_recv.recv()).await {
                        Ok(Some(data)) => {
                            if write_message(&mut write_stream, data).await.is_err() {
                                while conn_recv.recv().await.is_some() {
                                    // empty
                                }
                                break
                            }
                        }
                        Ok(None) => break,
                        Err(_) => {
                            if write_message(&mut write_stream, LinkOp::KeepAlive).await.is_err() {
                                while conn_recv.recv().await.is_some() {
                                    // empty
                                }
                                break
                            }
                        }
                    }
                }
            });
            let _provide_services = ProvideServices::new(services.clone(), provide, &conn_send, peer_addr);
            let mut link_target = HashMap::new();
            while let Some(op) = read_message::<LinkOp>(&mut read_stream, u32::MAX as usize).await? {
                log::trace!("Receive message from {}: {:?}", peer_addr, op);
                let allow_instant = Instant::now() - Duration::from_secs(UDP_TIMEOUT_SECS);
                services.lock().unwrap().udp_link_src_target.retain(|_, (_, _, x)| *x > allow_instant);
                match op {
                    LinkOp::Start { id, target_service, timeout_secs } => {
                        let target = services.lock().unwrap().map.get(&target_service).cloned();
                        match target {
                            Some(target) => {
                                services.lock().unwrap().link_src.insert(id, conn_send.clone());
                                if target.send(LinkOp::Start { id, target_service: target_service.clone(), timeout_secs }).await.is_err() {
                                    warn!("Data connection from {} requested an ended service {:?} (conn id {:?})", peer_addr, target_service, id);
                                    services.lock().unwrap().link_src.remove(&id);
                                    let _ = conn_send.send(LinkOp::Reject { id }).await;
                                } else {
                                    debug!("Data connection from {} requested service {:?} (conn id {:?})", peer_addr, target_service, id);
                                    link_target.insert(id, target.clone());
                                    let services = services.clone();
                                    let conn_send = conn_send.clone();
                                    tokio::task::spawn(async move {
                                        tokio::time::sleep(std::time::Duration::from_secs(timeout_secs as u64)).await;
                                        let target = services.lock().unwrap().link_src.remove(&id);
                                        if target.is_some() {
                                            warn!("Data connection from {} requested service {:?} timeout (conn id {:?})", peer_addr, target_service, id);
                                            let _ = conn_send.send(LinkOp::Reject { id }).await;
                                        }
                                    });
                                }
                            }
                            None => {
                                warn!("Data connection from {} requested an unregisted service {:?} (conn id {:?})", peer_addr, target_service, id);
                                let _ = conn_send.send(LinkOp::Reject { id }).await;
                            }
                        }
                    }
                    LinkOp::Accept { id } => {
                        let target = services.lock().unwrap().link_src.remove(&id);
                        if let Some(target) = target {
                            link_target.insert(id, target.clone());
                            if target.send(LinkOp::Accept { id }).await.is_err() {
                                warn!("Data connection from {} failed to build service (conn id {:?})", peer_addr, id);
                                link_target.remove(&id);
                                let _ = target.send(LinkOp::End { id }).await;
                            } else {
                                debug!("Data connection from {} accepted service (conn id {:?})", peer_addr, id);
                            }
                        } else {
                            warn!("Data connection from {} accepted an invalid or timeout client (conn id {:?})", peer_addr, id);
                        }
                    }
                    LinkOp::Reject { id } => {
                        let target = services.lock().unwrap().link_src.remove(&id);
                        if let Some(target) = target {
                            warn!("Data connection from {} rejected service (conn id {:?})", peer_addr, id);
                            let _ = target.send(LinkOp::Reject { id }).await;
                        }
                    }
                    LinkOp::End { id } => {
                        if let Some(target) = link_target.remove(&id) {
                            debug!("Data connection from {} end service (conn id {:?})", peer_addr, id);
                            let _ = target.send(LinkOp::End { id }).await;
                        }
                    }
                    LinkOp::Data { id, payload } => {
                        if let Some(target) = link_target.get(&id) {
                            if target.send(LinkOp::Data { id, payload }).await.is_err() {
                                debug!("Data connection failed to send data (conn id {:?})", id);
                                let _ = conn_send.send(LinkOp::End { id }).await;
                            }
                        }
                    }
                    LinkOp::UdpRequest { id, target_service, payload } => {
                        let target = {
                            let mut services = services.lock().unwrap();
                            if let Some((target, _, instant)) = services.udp_link_src_target.get_mut(&id) {
                                *instant = Instant::now();
                                Some(target.clone())
                            } else {
                                if let Some(s) = services.map.get(&target_service).cloned() {
                                    services.udp_link_src_target.insert(id, (s.clone(), conn_send.clone(), Instant::now()));
                                    Some(s)
                                } else {
                                    None
                                }
                            }
                        };
                        if let Some(target) = target {
                            if target.send(LinkOp::UdpRequest { id, target_service, payload }).await.is_err() {
                                debug!("Data connection failed to send UDP request packet (conn id {:?})", id);
                            }
                        }
                    }
                    LinkOp::UdpResponse { id, payload } => {
                        let target = {
                            let udp_link_src_target = &mut services.lock().unwrap().udp_link_src_target;
                            if let Some((_, target, instant)) = udp_link_src_target.get_mut(&id) {
                                *instant = Instant::now();
                                Some(target.clone())
                            } else {
                                None
                            }
                        };
                        if let Some(target) = target {
                            if target.send(LinkOp::UdpResponse { id, payload }).await.is_err() {
                                debug!("Data connection failed to send UDP response packet (conn id {:?})", id);
                            }
                        }
                    }
                    LinkOp::KeepAlive => {
                        debug!("Keep alive message received from control connection");
                    }
                }
            }
        }
    }
    Ok(())
}

async fn start() {
    let app_config = AppConfig::parse();
    let services = Arc::new(Mutex::new(Services {
        map: HashMap::new(),
        link_src: HashMap::new(),
        udp_link_src_target: HashMap::new(),
    }));
    let listener = TcpListener::bind(&app_config.addr).await.unwrap();
    info!("Server started on {}", listener.local_addr().unwrap());
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let passcode = app_config.passcode.clone();
                let services = services.clone();
                tokio::task::spawn(async move {
                    if let Err(err) = connection(passcode, services, stream).await {
                        warn!("A control connection quit with error: {}", err);
                    }
                });
            }
            Err(err) => {
                warn!("Cannot accept a connection: {}", err);
            }
        };
    }
}

fn main() {
    env_logger::init();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(start())
}
