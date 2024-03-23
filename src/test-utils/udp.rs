use std::{net::UdpSocket, sync::Arc, time::Duration};

fn main() {
    // response thread
    std::thread::spawn(|| {
        let r = Arc::new(UdpSocket::bind("0.0.0.0:12345").unwrap());
        let s = r.clone();
        let mut buf = [0; 65536];
        while let Ok((len, addr)) = r.recv_from(&mut buf) {
            let data: Vec<_> = buf[..len].into();
            s.send_to(&data, addr).unwrap();
        }
    });

    // request
    std::thread::sleep(Duration::new(1, 0));
    let r = Arc::new(UdpSocket::bind("0.0.0.0:0").unwrap());
    let s = r.clone();
    s.connect("127.0.0.1:12346").unwrap();
    println!("send: Hello!");
    s.send(b"Hello!").unwrap();
    let mut i = 0;
    let mut buf = [0; 65536];
    loop {
        let size = r.recv(&mut buf).unwrap();
        let data: Vec<_> = buf[..size].into();
        println!("recv: {:?}", String::from_utf8_lossy(&data));
        i += 1;
        let next_data = format!("test: {}", i);
        println!("send: {:?}", next_data);
        s.send(next_data.as_bytes()).unwrap();
    }
}
