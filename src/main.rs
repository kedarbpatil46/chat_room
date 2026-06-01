use std::{collections::HashMap, sync::Arc};

use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::{Mutex, broadcast},
};

struct User {
    user_name: String,
    message_count: u32,
    user_name_entered: bool,
}

impl User {
    pub fn new(name: &String) -> User {
        User {
            user_name: name.clone(),
            message_count: 0,
            user_name_entered: true,
        }
    }
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:8000").await.unwrap();

    let (tx, _) = broadcast::channel::<String>(100);

    let users = Arc::new(Mutex::new(HashMap::<String, User>::new()));

    println!("Chat server running");

    loop {
        let (socket, address) = match listener.accept().await {
            Ok(soc) => soc,
            Err(err) => {
                println!("Could not connect {}", err);
                continue;
            }
        };

        let (reader, mut writer) = socket.into_split();

        let mut reader = BufReader::new(reader);
        let tx = tx.clone();
        let val = users.clone();

        tokio::spawn(async move {
            let mut rx = tx.subscribe();
            let mut username = String::new();

            let prompt_username = format!("Enter your username: \n");
            writer.write_all(prompt_username.as_bytes()).await.unwrap();

            let mut line = String::new();
            loop {
                line.clear();

                let bytes_read = reader.read_line(&mut line).await.unwrap();

                if bytes_read == 0 {
                    print!("Invalid username");
                } else {
                    username = line.trim().to_string();
                    let new_user = User::new(&username);
                    let mut guard2 = val.lock().await;
                    guard2.insert(address.to_string(), new_user);
                    match tx.send(format!("[{}] joined the chat\n", line.trim().to_string())) {
                        Ok(_) => {}
                        Err(er) => println!("Error is {}", er),
                    }
                    break;
                }
            }

            let read_task = tokio::spawn(async move {
                let mut line = String::new();
                loop {
                    line.clear();

                    let bytes_read = reader.read_line(&mut line).await.unwrap();

                    if bytes_read == 0 {
                        print!("Connection Closed");
                        let mut guard3 = val.lock().await;
                        guard3.remove(&address.to_string());
                        match tx.send(format!("[{}] left the chat", username)) {
                            Ok(_) => {}
                            Err(er) => println!("Error is {}", er),
                        }
                        break;
                    }

                    match tx.send(format!("[{}] {}", username, line)) {
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
