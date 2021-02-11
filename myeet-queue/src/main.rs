use std::{
    io::Error as IoError,
    net::SocketAddr,
};

use futures::prelude::*;
use futures::{
    pin_mut,
    future,
    channel::mpsc::{unbounded},
};

use async_std::{
    net::{TcpListener,
        TcpStream,
    },
    task,
};

mod server;
use server::Server;

use myeetlib::myeet_types::QueueMessage;

async fn handle_connection(raw_stream: TcpStream, addr: SocketAddr, s: Server) {
    println!("got a connection!");
    let ws_stream = async_tungstenite::accept_async(raw_stream)
        .await
        .expect("Error during the websocket handshake occurred");
    println!("WebSocket connection established: {}", addr);

    let (tx, rx) = unbounded();
    s.add_peer(addr, tx);

    let (outgoing, incoming) = ws_stream.split();
    let mut connected_server_id : Option<String> = None;
    let broadcast_incoming = incoming.
    try_filter(|msg| {
        future::ready(!msg.is_close())
    })
    .try_for_each(|msg| {
        let incoming_message : QueueMessage;
        incoming_message = serde_json::from_str(msg.to_text().unwrap())
            .expect("Failed to parse");
        println!("{:?}", incoming_message);
        match incoming_message {
            QueueMessage::Identify{server_id: sid} => {
                connected_server_id = Some(sid);
                if let Some(sid) = &connected_server_id {
                    s.associate_uuid_with_peer(String::from(sid), addr);
                }
                else {
                    println!("Server ID is None, this shouldn't happen");
                }
            },
            QueueMessage::NotifyClient{client_id: cid} => {
                if let Some(sid) = &connected_server_id {
                    s.notify_client(sid.to_string(), cid);
                    println!("{:?}", s.routing_table);
                }
                else {
                    println!("No server ID!");
                }
            },
            QueueMessage::DropClient{client_id: cid} => {
                if let Some(_sid) = &connected_server_id {
                    s.remove_client(cid);
                    println!("{:?}", s.routing_table);
                }
                else {
                    println!("No server ID!");
                }
            },
            QueueMessage::NewChatRequest{client_id: cid, client_nick: nick} => {
                if let Some(_sid) = &connected_server_id {
                    if let Err(e) = s.new_chat(cid, nick) {
                        println!("failed to make new chat: {}", e);
                    }
                }
                else {
                    println!("No server ID!");
                }
            },
            QueueMessage::CancelChatRequest{client_id: cid} => {
                if let Some(_sid) = &connected_server_id {
                    s.cancel_chat(cid);
                }
                else {
                    println!("No server ID!");
                }
            },
            QueueMessage::ChatMessage {client_id, chat_id, message_text} => {
                println!("client_id: {:?}", &client_id);
                println!("chat_id: {:?}", &chat_id);
                println!("message_text: {:?}", &message_text);
                if let Err(e) = s.process_chat_message(client_id, chat_id, message_text) {
                    println!("Failed to process chat message: {}", e);
                }
            }
        }
        future::ok(())
    });

    let receive_from_others = rx.map(Ok).forward(outgoing);

    pin_mut!(broadcast_incoming, receive_from_others);

    future::select(broadcast_incoming, receive_from_others).await;

    /*
     * this is testing / debug stuff.
     * when a user server disconnects from the queue we remove it from the server's server_list
     */
    match connected_server_id {
        Some(m) => {
            s.remove_server(m);
            println!("{:?}", s.server_list.clone());
        },
        None => {
            println!("Oddly no server id");
        }
    }
}

async fn websocket_loop(server: Server) -> Result<(), IoError> {
    let addr = "127.0.0.1:8082".to_string();
    
    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.expect("Failed to bind.");
    println!("Listening on {}", addr);

    while let Ok((stream, addr)) = listener.accept().await {
        task::spawn(handle_connection(stream, addr, server.clone()));
    }

    Ok(())
}

fn main() -> Result<(), IoError> {

    println!("==Myeet Queue==");
    let url = "REDACTEDURL".to_owned();

    let server = server::new_server(url)
        .expect("Failed to create server");

    task::block_on(websocket_loop(server))
        .expect("failed to start websocket loop");

    Ok(())
}
