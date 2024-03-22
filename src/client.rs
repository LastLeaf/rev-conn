use log::{error, warn, info, debug};
use clap::Parser;
use tokio::net::{tcp, TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::{Instant, Duration};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

mod proto;
use proto::*;

const UDP_TIMEOUT_SECS: u64 = 300;

/// Start rev-conn client
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct AppConfig {
    /// The server address and port
    #[clap(short, long)]
    addr: String,
    /// The passcode of the server
    #[clap(short = 'c', long)]
    passcode: String,
    /// The services that will be provided to other clients, in `NAME:TCP:IP:PORT` format
    #[clap(short, long)]
    provide: Vec<String>,
    /// The services that will be redirected from other clients, in `NAME:TCP:IP:PORT` format
    #[clap(short, long)]
    r#use: Vec<String>,
}

struct ServiceDefinition {
    name: String,
    addr: String,
    link_kind: LinkKind,
}

impl From<&str> for ServiceDefinition {
    fn from(v: &str) -> Self {
        let slices = v.splitn(3, ':').collect::<Box<_>>();
        let name = slices[0].to_owned();
        assert_ne!(name.len(), 0);
        let link_kind = match slices[1].to_ascii_lowercase().as_str() {
            "tcp" => LinkKind::Tcp,
            "udp" => LinkKind::Udp,
            x => panic!("illegal link kind: {}", x),
        };
        let addr = slices[2].to_owned();
        Self { name, addr, link_kind }
    }
}

struct ControlConnection {
    addr: String,
    passcode: String,
    provide_services: Vec<ServiceDefinition>,
    stream: Mutex<Option<mpsc::Sender<LinkOp>>>,
    pending_link_req: Arc<Mutex<HashMap<LinkId, TcpStream>>>,
    udp_link_map: Mutex<HashMap<SocketAddr, (LinkId, Instant)>>,
    udp_link_rev_map: Mutex<HashMap<LinkId, (Arc<UdpSocket>, SocketAddr, Instant)>>,
}

impl ControlConnection {
    fn new(addr: &str, passcode: &str, provide_services: Vec<ServiceDefinition>) -> Arc<Self> {
        let ret = Arc::new(Self {
            addr: addr.into(),
            passcode: passcode.into(),
            provide_services,
            stream: Default::default(),
            pending_link_req: Default::default(),
            udp_link_map: Default::default(),
            udp_link_rev_map: Default::default(),
        });
        ret
    }

    async fn reconnect(self: &Arc<Self>) -> Result<(), ConnectionError> {
        // connect to server
        debug!("Reconnecting to server on {}", self.addr);
        let stream = TcpStream::connect(&self.addr).await?;
        let (mut read_stream, mut write_stream) = stream.into_split();
        info!("Connected to server on {}", self.addr);
        write_message(&mut write_stream, ClientStartInfo {
            version: PROTO_VERSION,
            passcode: self.passcode.clone(),
            kind: ClientConnKind::Control { provide: self.provide_services.iter().map(|x| x.name.clone()).collect() },
        }).await?;
        let (conn_send, mut conn_recv) = mpsc::channel(65536);
        *self.stream.lock().unwrap() = Some(conn_send.clone());

        // keep alive with server
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

        // the links
        let pending_link_req = self.pending_link_req.clone();
        let mut link_target = HashMap::new();
        let mut udp_link_target: HashMap<LinkId, (Arc<UdpSocket>, Instant)> = HashMap::new();
        fn handle_data_conn(id: LinkId, mut read_dest: tcp::OwnedReadHalf, conn_send: mpsc::Sender<LinkOp>) {
            let mut payload = Vec::with_capacity(65536);
            payload.resize(65536, 0);
            tokio::task::spawn(async move {
                loop {
                    match read_dest.read(&mut payload).await {
                        Err(_) | Ok(0) => {
                            let _ = conn_send.send(LinkOp::End { id }).await;
                            break;
                        }
                        Ok(size) => {
                            if conn_send.send(LinkOp::Data { id, payload: payload[0..size].into() }).await.is_err() {
                                break;
                            }
                        }
                    };
                }
            });
        }
        while let Some(op) = read_message::<LinkOp>(&mut read_stream, u32::MAX as usize).await? {
            // release timeout UDP sockets
            let allow_instant = Instant::now() - Duration::from_secs(UDP_TIMEOUT_SECS);
            udp_link_target.retain(|_, (_, x)| *x > allow_instant);
            self.udp_link_rev_map.lock().unwrap().retain(|_, (_, _, x)| *x > allow_instant);

            // handle messages
            match op {
                LinkOp::Start { id, target_service, timeout_secs: _ } => {
                    let def = self.provide_services.iter().find(|x| x.name == target_service);
                    match def {
                        Some(def) if def.link_kind == LinkKind::Tcp => {
                            if let Ok(dest) = TcpStream::connect(&def.addr).await {
                                if conn_send.send(LinkOp::Accept { id }).await.is_err() {
                                    warn!("Cannot accept connection to provided service {:?} (conn id {:?})", target_service, id);
                                } else {
                                    debug!("Connected to provided service {:?} (conn id {:?})", target_service, id);
                                    let (read_dest, write_dest) = dest.into_split();
                                    link_target.insert(id, write_dest);
                                    handle_data_conn(id, read_dest, conn_send.clone());
                                }
                            } else {
                                warn!("Cannot visit provided service {:?} (conn id {:?})", target_service, id);
                                let _ = conn_send.send(LinkOp::Reject { id }).await;
                            }
                        }
                        _ => {
                            warn!("Data connection requested an illegal service {:?} (conn id {:?})", target_service, id);
                            let _ = conn_send.send(LinkOp::Reject { id }).await;
                        }
                    }
                }
                LinkOp::Accept { id } => {
                    let dest = pending_link_req.lock().unwrap().remove(&id);
                    if let Some(dest) = dest {
                        debug!("Service using request accepted (conn id {:?})", id);
                        let (read_dest, write_dest) = dest.into_split();
                        link_target.insert(id, write_dest);
                        handle_data_conn(id, read_dest, conn_send.clone());
                    } else {
                        error!("Accepted an invalid using request (conn id {:?})", id);
                        let _ = conn_send.send(LinkOp::End { id }).await;
                    }
                }
                LinkOp::Reject { id } => {
                    let dest = pending_link_req.lock().unwrap().remove(&id);
                    if let Some(_) = dest {
                        warn!("Service using request rejected (conn id {:?})", id);
                    } else {
                        error!("Rejected an invalid using request (conn id {:?})", id)
                    }
                }
                LinkOp::End { id } => {
                    if let Some(_) = link_target.remove(&id) {
                        debug!("End provided service (conn id {:?})", id);
                    }
                }
                LinkOp::Data { id, payload } => {
                    if let Some(write_dest) = link_target.get_mut(&id) {
                        if write_dest.write_all(&payload).await.is_err() {
                            debug!("Failed to send data to requested service (conn id {:?})", id);
                            let _ = conn_send.send(LinkOp::End { id }).await;
                        }
                    }
                }
                LinkOp::UdpRequest { id, target_service, payload } => {
                    let socket_meta = if let Some(socket_meta) = udp_link_target.get_mut(&id) {
                        Some(socket_meta)
                    } else {
                        let def = self.provide_services.iter().find(|x| x.name == target_service);
                        if let Some(def) = def {
                            if let Ok(socket) = UdpSocket::bind("0.0.0.0:0").await {
                                if socket.connect(&def.addr).await.is_ok() {
                                    let send = Arc::new(socket);
                                    {
                                        let recv = send.clone();
                                        let id = id.clone();
                                        let conn_send = conn_send.clone();
                                        tokio::spawn(async move {
                                            let mut buf = Vec::new();
                                            buf.resize(65536, 0);
                                            loop {
                                                let sleep = tokio::time::sleep(Duration::from_secs(UDP_TIMEOUT_SECS));
                                                tokio::select! {
                                                    _ = sleep => { break }
                                                    size = recv.recv(&mut buf) => {
                                                        if let Ok(size) = size {
                                                            let payload = buf[..size].into();
                                                            if conn_send.send(LinkOp::UdpResponse { id, payload }).await.is_err() {
                                                                break;
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        });
                                    }
                                    Some(udp_link_target.entry(id).or_insert((send, Instant::now())))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    };
                    if let Some((socket, active_instant)) = socket_meta {
                        *active_instant = Instant::now();
                        if socket.send(&payload).await.is_err() {
                            debug!("Failed to send UDP packet to requested service (conn id {:?})", id);
                        }
                    }
                }
                LinkOp::UdpResponse { id, payload } => {
                    if let Some((socket, addr, active_instant)) = self.udp_link_rev_map.lock().unwrap().get_mut(&id) {
                        *active_instant = Instant::now();
                        if socket.send_to(&payload, *addr).await.is_err() {
                            debug!("Failed to send UDP packet to requested service (conn id {:?})", id);
                        }
                    }
                }
                LinkOp::KeepAlive => {
                    debug!("Keep alive message received from control connection");
                }
            }
        }
        Ok(())
    }

    fn reset(&self) {
        *self.stream.lock().unwrap() = None;
        self.pending_link_req.lock().unwrap().clear();
    }

    async fn use_tcp_service(&self, name: &str, stream: TcpStream) {
        let mut conn_send = self.stream.lock().unwrap().clone();
        if let Some(conn_send) = conn_send.as_mut() {
            let id = LinkId::new();
            if conn_send.send(LinkOp::Start { id, target_service: name.into(), timeout_secs: LINK_TIMEOUT }).await.is_err() {
                error!("Failed to connect to used service {:?}", name);
            } else {
                debug!("Connecting to used service {:?} (conn id {:?})", name, id);
                self.pending_link_req.lock().unwrap().insert(id, stream);
            }
        } else {
            error!("Failed to connect to used service {:?} because control connection lost", name);
        }
    }

    async fn use_udp_service(&self, name: &str, source_addr: SocketAddr, payload: Vec<u8>, socket: Arc<UdpSocket>) {
        let allow_instant = Instant::now() - Duration::from_secs(UDP_TIMEOUT_SECS);
        self.udp_link_map.lock().unwrap().retain(|_, (_, x)| *x > allow_instant);
        let mut conn_send = self.stream.lock().unwrap().clone();
        if let Some(conn_send) = conn_send.as_mut() {
            let (id, _) = self.udp_link_map.lock().unwrap()
                .entry(source_addr)
                .and_modify(|(_, x)| { *x = Instant::now() })
                .or_insert_with(|| {
                    let link_id = LinkId::new();
                    let now = Instant::now();
                    self.udp_link_rev_map.lock().unwrap().insert(link_id, (socket, source_addr, now));
                    (link_id, now)
                })
                .clone();
            if conn_send.send(LinkOp::UdpRequest { id, target_service: name.into(), payload }).await.is_err() {
                error!("Failed to send packet to used service {:?}", name);
            }
        } else {
            error!("Failed to connect to used service {:?} because control connection lost", name);
        }
    }
}

async fn start() {
    let app_config = AppConfig::parse();
    let provide_services = app_config.provide.iter().map(|x| x.as_str().into()).collect::<Vec<ServiceDefinition>>();
    let use_services = app_config.r#use.iter().map(|x| x.as_str().into()).collect::<Vec<ServiceDefinition>>();
    let control_conn = ControlConnection::new(app_config.addr.as_str(), app_config.passcode.as_str(), provide_services);
    {
        let control_conn = control_conn.clone();
        tokio::task::spawn(async move {
            futures::future::join_all(
                use_services.into_iter().map(|def| {
                    let control_conn = control_conn.clone();
                    async move {
                        let name = def.name;
                        match def.link_kind {
                            LinkKind::Tcp => {
                                let listener = TcpListener::bind(&def.addr).await.unwrap();
                                info!("TCP Service used on {}", def.addr);
                                tokio::task::spawn(async move {
                                    loop {
                                        match listener.accept().await {
                                            Ok((stream, _)) => {
                                                control_conn.use_tcp_service(&name, stream).await;
                                            }
                                            Err(err) => {
                                                warn!("Cannot accept a connection for service {:?}: {}", name, err);
                                            }
                                        };
                                    }
                                })
                            }
                            LinkKind::Udp => {
                                info!("UDP Service used on {}", def.addr);
                                let socket = Arc::new(UdpSocket::bind(&def.addr).await.unwrap());
                                let recv = socket.clone();
                                tokio::task::spawn(async move {
                                    let mut buf = Vec::new();
                                    buf.resize(65536, 0);
                                    loop {
                                        match recv.recv_from(&mut buf).await {
                                            Ok((size, addr)) => {
                                                let payload = buf[..size].into();
                                                control_conn.use_udp_service(&name, addr, payload, socket.clone()).await;
                                            }
                                            Err(err) => {
                                                warn!("Cannot receive a packet for service {:?}: {}", name, err);
                                            }
                                        }
                                    }
                                })
                            }
                        }
                    }
                })
            ).await;
        });
    }
    loop {
        if let Err(err) = control_conn.reconnect().await {
            error!("Connection to server ended with error: {}", err);
        }
        control_conn.reset();
        warn!("Connection to server ended. Retry in {} secs.", LINK_TIMEOUT);
        tokio::time::sleep(std::time::Duration::from_secs(LINK_TIMEOUT as u64)).await;
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
