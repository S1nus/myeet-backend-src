use std::{
    sync::{
        Mutex, Arc,
    },
    collections::HashMap,
    net::SocketAddr,
};

use async_tungstenite::tungstenite::protocol::Message;

use futures::{
    channel::mpsc::{UnboundedSender},
};

use mysql::prelude::*;
use mysql::{Pool};

use linked_hash_map::LinkedHashMap;

use flurry::{HashMap as ConcurrentHashMap, HashSet as ConcurrentHashSet};

use myeetlib::myeet_types::ServerMessage;
use myeetlib::myeet_error::MyeetErr;

use uuid::Uuid;

type Tx = UnboundedSender<Message>;

type PeerMap = Arc<Mutex<HashMap<SocketAddr, Tx>>>;
type UUIDMap = Arc<Mutex<HashMap<String, SocketAddr>>>;
type RoutingTable = Arc<ConcurrentHashMap<String, String>>;
type ServerList = Arc<ConcurrentHashSet<String>>;
type ChatQueue = Arc<Mutex<LinkedHashMap<String, String>>>;

#[derive(Clone, Debug)]
pub struct Server {
    peermap: PeerMap,
    uuidmap: UUIDMap,
    // remove pub after debugging
    pub routing_table: RoutingTable,
    pub server_list: ServerList,
    pub chat_queue: ChatQueue,
    chat_size: usize,
    mysql_pool: Pool,
}

pub fn new_server(url: String) -> Result<Server, MyeetErr> {
    Ok(
        Server {
            peermap: PeerMap::new(Mutex::new(HashMap::new())),
            uuidmap: UUIDMap::new(Mutex::new(HashMap::new())),
            routing_table: RoutingTable::new(ConcurrentHashMap::new()),
            server_list: ServerList::new(ConcurrentHashSet::new()),
            chat_queue: ChatQueue::new(Mutex::new(LinkedHashMap::new())),
            chat_size: 2,
            mysql_pool: Pool::new(url)?,
        }
    )
}

impl Server {

    pub fn add_server(&self, uuid: String) {
        self.server_list.insert(String::from(&uuid), &self.server_list.guard());
    }

    pub fn remove_server(&self, uuid: String) {
        self.server_list.remove(&uuid, &self.server_list.guard());
    }

    pub fn notify_client(&self, server_id: String, client_id: String) {
        self.routing_table.insert(client_id, server_id, &self.routing_table.guard());
    }

    pub fn remove_client(&self, client_id: String) {
        self.routing_table.remove(&client_id, &self.routing_table.guard());
    }

    pub fn new_chat(&self, client_id: String, nickname: String) -> Result<(), MyeetErr> {
        self.chat_queue.clone().lock().unwrap()
            .insert(client_id, nickname);
        if self.chat_queue.clone().lock().unwrap().len() >= self.chat_size {
            let uuid = Uuid::new_v4(); 
            println!("NEW CHAT");
            let mut conn = self.mysql_pool.get_conn()?;
            let mut users : Vec<(String, String)> = Vec::new();
            for _i in 0..self.chat_size {
                if let Some(front) = self.chat_queue.clone().lock().unwrap().pop_front() {
                    users.push(front);
                }
                else {
                    users.push(("".to_owned(), "".to_owned()));
                    println!("There was a None in the chat_queue, this should never happen");
                }
            }
            conn.exec_batch("INSERT INTO groupchatmemberships (user_id, groupchatid, nickname) VALUES (?, ?, ?)",
                users.iter().map(|entry| {
                    vec![entry.0.to_string(), uuid.to_string(), entry.1.to_string()]
                })
            )?;
            for user in users {
                let notification_message = ServerMessage::NewChatCreated {
                    client_id: user.0.to_owned(),
                    chat_id: uuid.to_string(),
                };
                self.route_message(user.0, Message::from(serde_json::to_string(&notification_message)?))?;
            }
        }
        else {
            println!("NOt enough peeps for a new chat yet");
        }
        Ok(())
    }

    pub fn cancel_chat(&self, client_id: String) {
        self.chat_queue.clone().lock().unwrap()
            .remove(&client_id);
    }

    pub fn add_peer(&self, ip: SocketAddr, stream: Tx) {
        self.peermap
            .clone()
            .lock()
            .unwrap()
            .insert(ip, stream);
    }

    pub fn associate_uuid_with_peer(&self, uuid: String, ip: SocketAddr) {
        self.uuidmap
            .clone()
            .lock()
            .unwrap()
            .insert(uuid, ip);
    }

    pub fn send_server_message(&self, server_uuid: String, message: Message) -> Result<(), MyeetErr> {
        let ip = self.uuidmap
            .clone()
            .lock()
            .unwrap()
            [&server_uuid];
        self.peermap
            .clone()
            .lock()
            .unwrap()
            [&ip]
            .unbounded_send(message)?;
        Ok(())
    }

    pub fn route_message(&self, client_uuid: String, message: Message) -> Result<(), MyeetErr> {
        if let Some(server_id) = self.routing_table.get(&client_uuid, &self.routing_table.guard()) {
            self.send_server_message(String::from(server_id), message)?;
        }
        else {
            println!("server ID was null");
        }
        Ok(())
    }

    pub fn get_nickname(&self, client_id: String, chat_id: String) -> Result<String, MyeetErr> {
        let mut conn = self.mysql_pool.get_conn()?;
        match conn.exec::<String, &str, (String, String)>
            ("SELECT nickname FROM groupchatmemberships WHERE user_id=? AND groupchatid=?", (client_id, chat_id)) {
            Ok(nicks) => {
                if let Some(nick) = &nicks.get(0) {
                    Ok(nick.to_string())
                }
                else {
                    Ok("nonick".to_owned())
                }
            },
            Err(_) => {
                Err(MyeetErr::with_text("Couldn't find user's nick"))
            }
        }
    }

    pub fn process_chat_message(&self, client_id: String, chat_id: String, message_text: String) -> Result<(), MyeetErr> {
        // insert the message into the database
        // *NOTE* I might want to move this functionality into the user servers.
        // Then, look up who's in the groupchat and route the message to the online members of that
        // groupchat.

        let nick = match self.get_nickname(client_id.to_owned(), chat_id.to_owned()) {
            Ok(n) => n,
            Err(e) => {
                println!("{:?}", e);
                "nonickfound".to_owned()
            },
        };

        let mut conn = self.mysql_pool.get_conn()?;
        conn.exec::<String, String, (String, usize, String, String)>
            ("INSERT INTO messages (from_user, sent_when, groupchat, message_text) values (?, ?, ?, ?)".to_owned(),
            (
                client_id.to_owned(),
                1500,
                String::from(&chat_id),
                message_text.to_owned(),
            )
        )?;

        println!("SELECT user_id FROM groupchatmemberships WHERE groupchatid={}", &chat_id);

        let groupchat_members : Vec<String> = conn.exec
            (
            "SELECT user_id FROM groupchatmemberships WHERE groupchatid=? AND user_id!=?".to_owned(),
            (String::from(&chat_id), String::from(&client_id))
        )?;
        println!("USER IDs:");
        println!("{:?}", groupchat_members);


        for client_id in groupchat_members {

            let rsm = ServerMessage::RoutedChatMessage {
                client_id: client_id.to_owned(),
                chat_id: chat_id.to_owned(),
                message_text: message_text.to_owned(),
                nick: nick.to_owned()
            };

            self.route_message(client_id.to_owned(), Message::from(serde_json::to_string(&rsm)?))?;
        }

        Ok(())
    }

}
