use std::{
    sync::{Arc, Mutex},
    net::SocketAddr,
    borrow::Cow,
};

use futures::prelude::*;
use futures::{
    pin_mut,
    future,
    channel::mpsc::{unbounded},
};
use futures::stream::{SplitStream};

use async_std::{
    net::{TcpListener,
        TcpStream,
    },
    task,
};

use async_tungstenite::async_std::connect_async;
use async_tungstenite::WebSocketStream;
use async_tungstenite::tungstenite::Error as TungsteniteError;

//use mysql::*;

mod server;
use server::Server;

use myeetlib::myeet_types::{ClientMessage, QueueMessage, OutgoingClientMessage, ServerMessage};
use myeetlib::myeet_error::MyeetErr;

type WebSocketRead = SplitStream<WebSocketStream<TcpStream>>;

async fn handle_connection(raw_stream: TcpStream, addr: SocketAddr, s: Server) -> Result<(), MyeetErr> {

    println!("got a connection!");

    let ws_stream = match async_tungstenite::accept_async(raw_stream).await {
        Ok(w) => w,
        Err(_e) => {
            return Err(MyeetErr::with_text("failed to upgrade TCP to websocket"));
        }
    };

    println!("WebSocket connection established: {}", addr);

    let (tx, rx) = unbounded();

    let mut cid : Option<String> = None;

    s.add_peer(addr, tx);

    let (outgoing, incoming) = ws_stream.split();

    let broadcast_incoming = incoming.
    try_filter(|msg| {
        future::ready(!msg.is_close())
    })
    .try_for_each(|msg| {
        let incoming_message : ClientMessage;
        incoming_message = serde_json::from_str(msg.to_text().unwrap())
            .unwrap();
        match incoming_message {
            ClientMessage::Auth { client_id } => {
                println!("Received auth from {}", client_id);
                cid = Some(client_id.to_owned());
                if let Err(e) = s.notify_queue_of_client(client_id.to_owned()) {
                    println!("Error notifying queue: {}", e);
                    return future::ready(Err(
                        TungsteniteError::Protocol(
                            Cow::Owned("error notifying queue".to_owned())
                    )))
                }
                s.associated_uuid(client_id.to_owned(), addr);
                let users_chats = s.get_user_chats(client_id.to_owned())
                    .unwrap();
                let ucl = OutgoingClientMessage::UserChatList {
                    user_chats: users_chats,
                    new_chat: None,
                };
                s.send_message_to_client(addr, ucl).unwrap();
                future::ok(())
            },
            ClientMessage::ChatMessage { client_id, message_text, chat_id } => {
                if let Err(e) = s.send_chat_to_queue(
                    client_id,
                    chat_id,
                    message_text,
                ) {
                    println!("Error sending chat to queue: {}", e);
                    return future::ready(Err(
                        TungsteniteError::Protocol(
                            Cow::Owned("failed to send message to queue".to_owned())
                    )))
                }
                future::ok(())
            },
            ClientMessage::NewChatRequest { client_id, client_nick } => {
                println!("got a new chat request");
                let to_q = QueueMessage::NewChatRequest {
                    client_nick: client_nick,
                    client_id: client_id,
                };
                if let Err(e) = s.send_message_to_queue(to_q) {
                    println!("Failed to send message to queue: {}", e);
                    return future::ready(Err(
                        TungsteniteError::Protocol(
                            Cow::Owned("failed to send message to user".to_owned())
                    )))
                }
                future::ok(())
            },
            ClientMessage::CancelChatRequest { client_id } => {
                println!("got a cancel chat request");
                let to_q = QueueMessage::CancelChatRequest {
                    client_id: client_id
                };
                if let Err(e) = s.send_message_to_queue(to_q) {
                    println!("Failed to send message to queue: {}", e);
                    return future::ready(Err(
                        TungsteniteError::Protocol(
                            Cow::Owned("failed to send message to queue".to_owned())
                    )))
                }
                future::ok(())
            }
        }
        //future::ok(())
    });

    let receive_from_others = rx.map(Ok).forward(outgoing);

    pin_mut!(broadcast_incoming, receive_from_others);

    future::select(broadcast_incoming, receive_from_others).await;

    // better cancel their chat search just incase:
    s.remove_client(addr);
    if let Some(client_id) = cid {
        let to_q = QueueMessage::CancelChatRequest {
            client_id: client_id.to_owned()
        };
        if let Err(e) = s.send_message_to_queue(to_q) {
            println!("Error sending message to queue: {}", e);
        }
        if let Err(e) = s.drop_client_from_queue(client_id.to_owned()) {
            println!("Error sending message to queue: {}", e);
        }
    }
    else {
        println!("No client id? Really sus");
    }
    Ok(())

}

async fn websocket_loop(server: Server) -> Result<(), MyeetErr> {

    let addr = "127.0.0.1:8081".to_string();
    let try_socket = TcpListener::bind(&addr).await;
    let listener = try_socket.expect("Failed to bind.");
    println!("Listening on {}", addr);

    while let Ok((stream, addr)) = listener.accept().await {
        task::spawn(handle_connection(stream, addr, server.clone()));
    }

    Ok(())
}

async fn connect_to_queue() -> WebSocketStream<TcpStream> {

    let queue_addr = "ws://127.0.0.1:8082".to_string();
    let (queue_stream, _) = connect_async(queue_addr)
        .await
        .expect("Failed to connect.");

    println!("Connected to MQueue!");

    queue_stream

}

async fn queue_loop(read: WebSocketRead, s: Server) -> Result<(), MyeetErr> {

    let _queue_incoming = read
        .try_filter(|msg| {
            future::ready(!msg.is_close())
        })
        .try_for_each(|msg| {
            println!("{}", msg);
            let incoming_message : ServerMessage;
            incoming_message = serde_json::from_str(msg.to_text().unwrap())
                .unwrap();
            match incoming_message {
                ServerMessage::RoutedChatMessage { client_id, chat_id, message_text, nick } => {
                    let cm = OutgoingClientMessage::ChatMessage {
                        from: nick,
                        text: message_text,
                        when: 0,
                        chat_id: chat_id,
                    };
                    if let Err(e) = s.send_message_to_client_uuid(client_id, cm) {
                        println!("Error sending message to user: {}", e);
                    }
                },
                ServerMessage::NewChatCreated { client_id, chat_id } => {
                    println!("New chat created, gotta send its details to the user");
                    let users_chats = s.get_user_chats(client_id.to_owned())
                        .unwrap();
                    let ucl = OutgoingClientMessage::UserChatList {
                        user_chats: users_chats,
                        new_chat: Some(chat_id.to_owned()),
                    };
                    s.send_message_to_client_uuid(client_id, ucl).unwrap();
                }
            }
            future::ok(())
        }).await;

    println!("Queue loop end :(");

    Ok(())
}

fn main() -> Result<(), MyeetErr> {

    let url = "**REDACTEDURL";

    let queue_connection = task::block_on(connect_to_queue());
    let (queue_write, queue_read) = queue_connection.split();
    let arc_queue_write = Arc::new(Mutex::new(queue_write));

    let server = server::new_server(url.to_owned(), arc_queue_write)
        .expect("Failed to initialize server.");

    server.identify_to_queue()
        .expect("failed to send identify message to queue");

    println!("{}", server.uuid);

    task::spawn(queue_loop(queue_read, server.clone()));

    task::block_on(websocket_loop(server.clone()))
        .expect("failed to run webocket loop");

    Ok(())
}
