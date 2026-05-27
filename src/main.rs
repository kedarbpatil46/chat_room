use std::{collections::HashMap, os::unix::net::SocketAddr, sync::Arc};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::{Mutex, broadcast},
};

struct User {
    user_name: String,
    message_count: u32,
}

impl User {

    pub fn new(name: String) -> User {
        User {
            user_name: name,
            message_count: 0
        }
    }

    pub fn find_user() {}
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:8000").await.unwrap();

    let (tx, _) = broadcast::channel::<String>(100);

    let users = Arc::new(Mutex::new(HashMap::<String, User>::new()));

    println!("Chat server running");

    loop {
        let (mut socket, address) = match listener.accept().await {
            Ok(soc) => soc,
            Err(err) => {
                println!("Could not connect {}", err);
                continue;
            }
        };

        let user_joined_msg = format!("{} joined the chat\n", address);
        socket.write_all(user_joined_msg.as_bytes()).await.unwrap();

        let (reader, mut writer) = socket.into_split();

        let tx = tx.clone();
        let users = users.clone();

        tokio::spawn(async move {
            let mut rx = tx.subscribe();

            let read_task = tokio::spawn(async move {
                let guard = users.lock().await;
                let user = guard.get(&address.to_string());
                match user {
                    Some(user) => user,
                    None => {
                        let mut reader = BufReader::new(reader);

                        let mut line = String::new();
                        loop {
                            line.clear();

                            let bytes_read = reader.read_line(&mut line).await.unwrap();       
                        }
                    }
                }

                let mut reader = BufReader::new(reader);

                let mut line = String::new();
                loop {
                    line.clear();

                    let bytes_read = reader.read_line(&mut line).await.unwrap();

                    if bytes_read == 0 {
                        print!("Connection Closed");
                        break;
                    }

                    match tx.send(format!("[{}] {}", address, line)) {
                        Ok(_) => {}
                        Err(er) => println!("Error is {}", er),
                    }
                }
            });

            let write_task = tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(msg) => writer.write_all(msg.as_bytes()).await.unwrap(),
                        Err(err) => {
                            println!("Could not send message {}", err);
                            break;
                        }
                    }
                }
            });

            read_task.await.unwrap();
            write_task.await.unwrap();
        });
    }
}
