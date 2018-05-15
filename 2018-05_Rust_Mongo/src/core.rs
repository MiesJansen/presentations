extern crate hostname;
extern crate thread_id;

use std::process;
use std::io::{Result, Error, ErrorKind};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn get_my_id() -> Result<String> {
    Ok(format!("{}-{}-{}",
               hostname::get_hostname().ok_or(Error::new(ErrorKind::Other, format!("Cannot read hostname")))?,
               process::id(),
               thread_id::get()
    ))
}

pub fn timestamp_ms() -> Result<u64>  {
    let current_time = SystemTime::now().duration_since(UNIX_EPOCH)
        .map_err((|err| Error::new(ErrorKind::Other, format!("cannot create timestamp: {}",err))))?;
    Ok(current_time.as_secs() * 1000 + current_time.subsec_nanos() as u64 / 1000 / 1000)
}