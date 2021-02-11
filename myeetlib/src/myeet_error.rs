use std::error::Error;
use std::fmt;

use mysql::Error as MysqlError;
use serde_json::Error as SerdeJsonError;
use async_tungstenite::tungstenite::Error as TungsteniteError;
use async_tungstenite::tungstenite::Message;
use futures::channel::mpsc::{SendError, TrySendError};

#[derive(Debug)]
pub struct MyeetErr {
    message: Option<String>
}

impl MyeetErr {
    pub fn with_text(text: &str) -> MyeetErr {
        MyeetErr {
            message: Some(String::from(text))
        }
    }
}

impl Error for MyeetErr {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None    
    }
}

impl fmt::Display for MyeetErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(s) = &self.message {
            write!(f, "{}", s)
        }
        else {
            write!(f, "no error info")
        }
    }
}

impl From<MysqlError> for MyeetErr {
    fn from(_m: MysqlError) -> Self {
        MyeetErr {
            message: Some("Error with database".to_owned())
        }
    }
}

impl From<SerdeJsonError> for MyeetErr {
    fn from(_m: SerdeJsonError) -> Self {
        MyeetErr {
            message: Some("Error parsing JSON".to_owned())
        }
    }
}

impl From<TungsteniteError> for MyeetErr {
    fn from(_m: TungsteniteError) -> Self {
        MyeetErr {
            message: Some("Websocket error".to_owned())
        }
    }
}

impl From<SendError> for MyeetErr {
    fn from(_m: SendError) -> Self {
        MyeetErr {
            message: Some("Send Error".to_owned())
        }
    }
}

impl From<TrySendError<Message>> for MyeetErr {
    fn from(_m: TrySendError<Message>) -> Self {
        MyeetErr {
            message: Some("TrySend Error".to_owned())
        }
    }
}
