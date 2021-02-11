use std::{
    sync::{
        Mutex, Arc,
    },
    collections::HashMap,
    net::SocketAddr,
};

use async_tungstenite::tungstenite::protocol::Message;
use async_tungstenite::WebSocketStream;

use futures::prelude::*;
use futures::{
    channel::mpsc::{UnboundedSender},
    stream::{SplitSink},
};

use async_std::{
    net::{
        TcpStream,
    },
    task,
};

use uuid::Uuid;

use mysql::prelude::*;
use mysql::{Pool};

use myeetlib::myeet_types::{QueueMessage, OutgoingClientMessage};
use myeetlib::myeet_error::MyeetErr;

type Tx = UnboundedSender<Message>;

type PeerMap = Arc<Mutex<HashMap<SocketAddr, Tx>>>;
type UUIDMap = Arc<Mutex<HashMap<String, SocketAddr>>>;

type WebSocketWrite = SplitSink<WebSocketStream<TcpStream>, Message>;

#[derive(Clone, Debug)]
pub struct Server {
    pub uuid: String,
    peermap: PeerMap,
    uuidmap: UUIDMap,
    mysql_pool: Pool,
    queue_write: Arc<Mutex<WebSocketWrite>>,
}

pub fn new_server(url: String, queue_write: Arc<Mutex<WebSocketWrite>>) -> Result<Server, MyeetErr> {
    Ok(
        Server {
            uuid: Uuid::new_v4().to_string(),
            peermap: PeerMap::new(Mutex::new(HashMap::new())),
            uuidmap: UUIDMap::new(Mutex::new(HashMap::new())),
            mysql_pool: Pool::new(url)?,
            queue_write: queue_write,
        }
    )
}

impl Server {

    pub fn add_peer(&self, ip: SocketAddr, stream: Tx) {
        self.peermap
            .clone()
            .lock()
            .unwrap()
            .insert(SocketAddr::from(ip), stream);
    }

    pub fn associated_uuid(&self, uuid: String, addr: SocketAddr) {
        self.uuidmap
            .clone()
            .lock()
            .unwrap()
            .insert(uuid, SocketAddr::from(addr));
    }

    pub fn remove_client(&self, ip: SocketAddr) {
        self.peermap
            .lock()
            .unwrap()
            .remove(&SocketAddr::from(ip));
    }

    pub fn send_message_to_queue(&self, m: QueueMessage) -> Result<(), MyeetErr> {
        task::block_on(
            self.queue_write.lock().unwrap().send(
                Message::from(
                    serde_json::to_string(&m)?
                )
            )
        )?;

        Ok(())
    }

    // Is it okay to use block_on in this way? I am not sure.
    pub fn notify_queue_of_client(&self, client_id: String) -> Result<(), MyeetErr> {
        let nc = QueueMessage::NotifyClient {
            client_id: client_id.to_owned(),
        };
        self.send_message_to_queue(nc)?;
        
        Ok(())
    }

    pub fn identify_to_queue(&self) -> Result<(), MyeetErr> {
        let id = QueueMessage::Identify {
            server_id: self.uuid.to_string(),
        };
        self.send_message_to_queue(id)?;
        Ok(())
    }

    pub fn drop_client_from_queue(&self, client_id: String) -> Result<(), MyeetErr> {
        let dc = QueueMessage::DropClient {
            client_id: client_id.to_owned(),
        };
        self.send_message_to_queue(dc)?;
        Ok(())
    }

    pub fn send_chat_to_queue(&self, client_id: String, chat_id: String, message_text: String) -> Result<(), MyeetErr> {
        let cm = QueueMessage::ChatMessage {
            client_id: client_id.to_owned(),
            chat_id: chat_id.to_owned(),
            message_text: message_text.to_owned(),
        };
        self.send_message_to_queue(cm)?;
        Ok(())
    }

    pub fn get_nicks(&self, chat_id: String) -> Result<Vec<String>, MyeetErr> {
        let mut conn = self.mysql_pool.get_conn()?;

        let nicks : Vec<String> = conn
            .exec::<String, &str, (String,)>(
                "SELECT nickname FROM groupchatmemberships WHERE groupchatid=?",
                (chat_id.to_owned(),)
            )?;

        Ok(nicks)
        
    }

    pub fn get_user_chats(&self, client_id: String) -> Result<Vec<OutgoingClientMessage>, MyeetErr> {
        let mut conn = self.mysql_pool.get_conn()?;

        let users_chats: Vec<String> = conn
            .exec::<String, &str, (String,)>(
                "SELECT groupchatid FROM groupchatmemberships WHERE user_id=?",
                (client_id.to_owned(),)
        )?;

        let outgoing : Result<Vec<OutgoingClientMessage>, MyeetErr> = users_chats
            .iter()
            .map(|gid| {
                let nicks = match self.get_nicks(gid.to_owned()) {
                    Ok(nicknames) => nicknames,
                    Err(_) => vec!["unknown participants".to_owned()],
                };
                Ok(
                    OutgoingClientMessage::UserChat{
                        id: gid.to_owned(),
                        messages: self.get_messages_from_chat_id(gid.to_owned(), Some(client_id.to_owned()))?,
                        people: nicks,
                    }
                )
            })
            .collect();

        outgoing
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

    pub fn get_messages_from_chat_id(&self, chat_id: String, me: Option<String>) -> Result<Vec<OutgoingClientMessage>, MyeetErr> {
        let mut conn = self.mysql_pool.get_conn()?;

        let messages : Vec<OutgoingClientMessage> = match me {
            Some(me_id) => {
                conn
                    .exec::<(String, String, usize), &str, (String, String)>(
                        "select case when (user_id=?) then 'me' else nickname end as nickname, message_text, sent_when from messages left join groupchatmemberships on groupchatmemberships.user_id=messages.from_user and groupchatmemberships.groupchatid=messages.groupchat WHERE groupchatid=?",
                        (me_id.to_owned(), chat_id.to_owned(),)
                )?
                    .iter()
                    .map(|(nick, text, time)| {
                        OutgoingClientMessage::ChatMessage {
                            from: nick.to_owned(),
                            text: text.to_owned(),
                            when: *time,
                            chat_id: chat_id.to_owned(),
                        }
                    })
                    .collect()
            },
            None => {
                conn
                    .exec::<(String, String, usize), &str, (String,)>(
                        "select nickname, message_text, sent_when from messages left join groupchatmemberships on groupchatmemberships.user_id=messages.from_user WHERE groupchatid=?",
                        (chat_id.to_owned(),)
                )?
                    .iter()
                    .map(|(nick, text, time)| {
                        OutgoingClientMessage::ChatMessage {
                            from: nick.to_owned(),
                            text: text.to_owned(),
                            when: *time,
                            chat_id: chat_id.to_owned(),
                        }
                    })
                    .collect()

            }
        };

        Ok(messages)
    }

    pub fn send_message_to_client_uuid(&self, uuid: String, m: OutgoingClientMessage) -> Result<(), MyeetErr> {
        if let Some(addr) = 
            self.uuidmap
                .lock()
                .unwrap()
                .get(&uuid)
        {
            println!("got addr: {:?}", addr);
            if let Ok(()) = self.send_message_to_client(addr.to_owned(), m) {
                println!("sent succesfully");
            }
            else {
                println!("error");
            }
        }
        else {
            println!("no addr?");
        }
        Ok(())
    }

    pub fn send_message_to_client(&self, client_addr: SocketAddr, m: OutgoingClientMessage) -> Result<(), MyeetErr> {
        if let Some(mut sender) =
            self.peermap
                .lock()
                .unwrap()
                .get(&client_addr)
        {
            task::block_on(
                sender.send(
                    Message::from(
                        serde_json::to_string(&m)?
                    )
                )
            )?;
            Ok(())
        }
        else {
            Err(MyeetErr::with_text("no connection to this peer"))
        }
    }

}
