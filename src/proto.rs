use serde::{Serialize, Deserialize};
use tokio::net::tcp;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bincode::Options;

pub(crate) const LINK_TIMEOUT: u32 = 20;
pub(crate) const KEEP_ALIVE_INTERVAL: u32 = 10;
pub(crate) const KEEP_ALIVE_TIMEOUT: u32 = 20;
pub(crate) const PROTO_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
pub(crate) struct ClientStartInfo {
    pub(crate) version: u32,
    pub(crate) passcode: String,
    pub(crate) kind: ClientConnKind,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum ClientConnKind {
    Control {
        provide: Vec<String>,
    },
}

#[derive(Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct LinkId([u32; 4]);

impl LinkId {
    pub(crate) fn new() -> Self {
        let uuid = uuid::Uuid::new_v4().as_u128();
        Self([
            (uuid >> 96) as u32,
            (uuid >> 64) as u32,
            (uuid >> 32) as u32,
            (uuid >> 0) as u32,
        ])
    }
}

impl std::fmt::Debug for LinkId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:x}{:x}{:x}{:x}", self.0[0], self.0[1], self.0[2], self.0[3])
    }
}

#[derive(Serialize, Deserialize)]
pub(crate) enum LinkOp {
    Start {
        id: LinkId,
        target_service: String,
        timeout_secs: u32,
    },
    Accept {
        id: LinkId,
    },
    Reject {
        id: LinkId,
    },
    End {
        id: LinkId,
    },
    Data {
        id: LinkId,
        payload: Vec<u8>,
    },
    KeepAlive,
}

pub(crate) async fn read_message<T: serde::de::DeserializeOwned>(
    stream: &mut tcp::OwnedReadHalf,
    max_size: usize,
) -> Result<Option<T>, ConnectionError> {
    let size = match tokio::time::timeout(std::time::Duration::from_secs(KEEP_ALIVE_TIMEOUT as u64), stream.read_u32()).await? {
        Ok(x) => x as usize,
        Err(err) => {
            if err.kind() == tokio::io::ErrorKind::UnexpectedEof {
                return Ok(None);
            }
            return Err(err.into());
        }
    };
    if size > max_size {
        return Ok(None);
    }
    let mut buf = Vec::with_capacity(size);
    buf.resize(size, 0);
    tokio::time::timeout(std::time::Duration::from_secs(KEEP_ALIVE_TIMEOUT as u64), stream.read_exact(&mut buf)).await??;
    let ret = bincode::DefaultOptions::new().deserialize::<T>(&buf)?;
    Ok(Some(ret))
}

pub(crate) async fn write_message<T: serde::ser::Serialize>(
    stream: &mut tcp::OwnedWriteHalf,
    msg: T,
) -> Result<(), ConnectionError> {
    let buf = bincode::DefaultOptions::new().serialize(&msg)?;
    let len = buf.len();
    if len > u32::MAX as usize {
        Err(ConnectionError::Custom("Message too long".into()))?;
    }
    stream.write_u32(len as u32).await?;
    stream.write_all(&buf).await?;
    Ok(())
}

pub(crate) enum ConnectionError {
    IoError(std::io::Error),
    TimeoutError(tokio::time::error::Elapsed),
    BincodeError(bincode::Error),
    Custom(String),
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IoError(x) => write!(f, "{}", x),
            Self::TimeoutError(x) => write!(f, "{}", x),
            Self::BincodeError(x) => write!(f, "{}", x),
            Self::Custom(x) => write!(f, "{}", x),
        }
    }
}

impl From<std::io::Error> for ConnectionError {
    fn from(v: std::io::Error) -> Self {
        ConnectionError::IoError(v)
    }
}

impl From<tokio::time::error::Elapsed> for ConnectionError {
    fn from(v: tokio::time::error::Elapsed) -> Self {
        ConnectionError::TimeoutError(v)
    }
}

impl From<bincode::Error> for ConnectionError {
    fn from(v: bincode::Error) -> Self {
        ConnectionError::BincodeError(v)
    }
}
