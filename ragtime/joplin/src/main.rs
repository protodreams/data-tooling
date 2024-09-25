use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Server listening on http://127.0.0.1:8080");

    loop {
        let (mut socket, addr) = listener.accept().await?;
        println!("New connection from: {}", addr);

        tokio::spawn(async move {
            let mut buffer = vec![0; 1024];
            let mut request = Vec::new();

            loop {
                match socket.read(&mut buffer).await {
                    Ok(n) if n == 0 => return,
                    Ok(n) => {
                        println!("Read {} bytes from {}", n, addr);
                        request.extend_from_slice(&buffer[..n]);
                        if request.windows(4).any(|window| window == b"\r\n\r\n") {
                            break;
                        }
                    }

                    Err(e) => {
                        eprintln!("failed to read from socket; err = {:?}", e);
                        return;
                    }
                }
            }

            let request_str = String::from_utf8_lossy(&request);

            println!("Received request: {}", request_str);

            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\nYou sent {}",
                request_str.len() + 10,
                request_str
            );

            socket.write_all(response.as_bytes()).await.unwrap();
            println!("Sent response to {}", addr);
        });
    }
}
