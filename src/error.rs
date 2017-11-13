
use std::io;

// TODO: replace the placeholder with a real error type
pub enum ErrorType {
    IO(io::Error),
    Msg(String),
}
