use std::ops::Deref;
use std::sync::Arc;
use std::env;
use std::io::{Result, Error, ErrorKind};

use mongo_driver::client::{Client,ClientPool,Uri};
use mongo_driver::database::Database;
use bson::{Bson};

static MONGO_URI_KEY: &'static str = "MONGO_URL";
pub const MONGO_UNIQUE_INDEX_VIOLATION: i32 = 11000;

pub struct DbConn<'a>(pub Client<'a>);

impl<'a> Deref for DbConn<'a> {
    type Target = Client<'a>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub type Pool = Arc<ClientPool>;

fn get_uri() -> Uri {
    Uri::new(env::var(MONGO_URI_KEY).expect(&format!("Cannot read: {}", MONGO_URI_KEY))).expect(&format!("Cannot create db URI: {}", MONGO_URI_KEY))
}

pub fn init_pool() -> Pool {
    let uri = get_uri();
    Arc::new(ClientPool::new(uri, None))
}

pub fn get_db<'a>(conn: &'a DbConn<'a>) -> Database<'a> {
    let uri = get_uri();
    let db_name = uri.get_database().expect("Please supply a db name.");
    conn.get_database(db_name.as_bytes())
}

pub fn update_indexes(db: &Database, collection_name: &str, indexes: Bson) -> Result<()> {
    let command =
        doc! {
    "createIndexes": collection_name,
    "indexes": indexes
    };
    db.command_simple(command.clone(), None)
        .map_err(|err| Error::new(ErrorKind::Other, format!("cannot create product mutex indexes: {:?}, err: {}", command, err)))?;

    Ok(())
}