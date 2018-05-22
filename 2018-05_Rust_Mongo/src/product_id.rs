use std::io::{self, Error, ErrorKind};
use mongo_driver::database::Database;
use bson::{self, Bson};
use uuid::Uuid;

use product::{PRODUCT_COLL_NAME};
use core;
use mongo;

pub const CRYSTAL_PRODUCT_INTERNAL_ID_KEY: &'static str = "_crystalProductId";
const CRYSTAL_METADATA_MUTEX_COLL_NAME: &'static str = "product_id_mutexes";
const PRODUCT_ID_FIELDNAME: &'static str = "product_id_fieldname";
const PRODUCT_ID_VAL: &'static str = "product_id";
const SUBMITTER_ID: &'static str = "submitter_id";

type CrystalProductId = String;
#[derive(PartialEq)]
pub enum CrystalProductIdState {
    ClearMutex,
    Existing,
}

fn get_from_product_table(db: &Database, id_field_name: &str, id_val: &str) -> io::Result<Option<CrystalProductId>>{
    let cmd = doc! {
        "find": PRODUCT_COLL_NAME,
        "filter": doc! {
            id_field_name: id_val
        }
    };
    let doc_opt = db.command_batch(cmd.clone(), None)
        .map_err(|err|
            Error::new(
                ErrorKind::Other,
                format!("cannot read product id from db: {}  cmd: {:?}",err, cmd)
            )
        )?.next();

    if let Some(doc_result) = doc_opt {
        let doc = doc_result
            .map_err(|err|
                Error::new(
                    ErrorKind::Other,
                    format!("cannot read result from db: {}  cmd: {:?}", err, cmd)
                )
            )?;
        let c_id = doc
            .get_str(CRYSTAL_PRODUCT_INTERNAL_ID_KEY)
            .map_err(|err|
                Error::new(
                    ErrorKind::Other,
                    format!("cannot find crystal product id: {}  cmd: {:?}", err, cmd)
                )
            )?;
        Ok(Some(c_id.to_owned()))
    } else {
        Ok(None)
    }
}

pub type Timestamp = i64;

#[derive(Debug, Serialize)]
struct ProductIdMutex<'a> {
    submitter_id: String,
    #[serde(rename = "_crystalProductId")]
    crystal_id: Uuid,
    product_id_fieldname: &'a str,
    product_id: &'a str,
    last_updated_utc: Timestamp
}

#[derive(Deserialize, Debug)]
struct WriteError {
    index: i32,
    code: i32
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct InsertResponse {
    n: i32,
    write_errors: Option<Vec<WriteError>>
}

fn lock(db: &Database, product_id_fieldname: &str, product_id: &str) -> io::Result<(CrystalProductId,CrystalProductIdState)>{
    //       * if you can't then get it from the mutex and add it
    //      * check again that's not in the product table
    let mutex = ProductIdMutex {
        submitter_id: core::get_my_id()
            .map_err(|err| Error::new(ErrorKind::Other, format!("Cannot read my process id: {}",err)))?,
        crystal_id: Uuid::new_v4(),
        product_id_fieldname,
        product_id,
        last_updated_utc: core::timestamp_ms()? as i64
    };
    let mutex_bson = bson::to_bson(&mutex)
        .map_err(|err| Error::new(ErrorKind::Other, format!("Cannot convert to bson: {}",err)))?;
    let cmd = doc!{
        "insert": CRYSTAL_METADATA_MUTEX_COLL_NAME,
        "documents": vec![mutex_bson],
        "ordered": false
    };
    let insert_response_doc = db.command_simple(cmd, None)
        .map_err(|err| Error::new(ErrorKind::Other, format!("Cannot insert mutex: {}",err)))?;
    println!("lock insert: {:?}", insert_response_doc);
    let ir: InsertResponse = bson::from_bson(Bson::Document(insert_response_doc))
        .map_err(|err| Error::new(ErrorKind::Other, format!("Cannot parse result from db: {}",err)))?;

    if let Some(wes) = ir.write_errors {
        if wes[0].code == mongo::MONGO_UNIQUE_INDEX_VIOLATION {
            let c_id_opt = get_from_product_table(db, product_id_fieldname, product_id)?;
            if let Some(c_id) = c_id_opt {
                Ok((c_id, CrystalProductIdState::Existing))
            } else {
                let c_id_result = db.command_batch(doc! {
                    "find":CRYSTAL_METADATA_MUTEX_COLL_NAME,
                    "filter": doc!{
                        PRODUCT_ID_FIELDNAME: product_id_fieldname,
                        PRODUCT_ID_VAL: product_id
                    }
                }, None);
                let c_id_opt = c_id_result
                    .map_err(|err| Error::new(ErrorKind::Other, format!("Cannot read mutex from db: {}", err)))?
                    .next();
                if let Some(c_id) = c_id_opt {
                    Ok((c_id
                            .map_err(|err| Error::new(ErrorKind::Other, format!("Error loading crystal id from db: {}", err)))?
                            .get_str(CRYSTAL_PRODUCT_INTERNAL_ID_KEY)
                            .map_err(|err| Error::new(ErrorKind::Other, format!("Cannot find crystal id: {}", err)))?
                            .to_owned(),
                        CrystalProductIdState::Existing))
                } else {
                    let c_id_opt = get_from_product_table(db, product_id_fieldname, product_id)?;
                    if let Some(c_id) = c_id_opt {
                        Ok((c_id, CrystalProductIdState::Existing))
                    } else {
                        Err(Error::new(ErrorKind::Other, format!("Cannot find crystal id or insert a mutex to add one: field: {}, val: {}", product_id_fieldname, product_id)))
                    }
                }
            }
        } else {
            Err(Error::new(ErrorKind::Other, format!("Error writing the mutex: {:?}", wes)))
        }
    } else {
        // now I own the mutex, so I need to clear it
        // check one more time in the product table
        let c_id_opt = get_from_product_table(db, product_id_fieldname, product_id)?;
        if let Some(c_id) = c_id_opt {
            Ok((c_id, CrystalProductIdState::ClearMutex))
        } else {
            Ok((mutex.crystal_id.to_string(), CrystalProductIdState::ClearMutex))
        }
    }
}

pub fn get(db: &Database, external_id_fieldname: &str, external_id_val: &str) -> io::Result<(CrystalProductId,CrystalProductIdState)>{
    //ensure no duplicate ids
    // * check the product table
    let c_id_opt = get_from_product_table(db, external_id_fieldname, external_id_val)?;
    if let Some(c_id) = c_id_opt {
        //   * if it's there
        Ok((c_id,CrystalProductIdState::Existing))
    } else {
        //   * if it's not there...
        //      * Add it!
        //      * lock it
        lock(db, external_id_fieldname, external_id_val)
    }
}

pub fn ensure_mutex_indicies(db: &Database) -> io::Result<()> {
    let indexes = Bson::Array(vec![
        Bson::Document( doc! {
                    "key": {PRODUCT_ID_FIELDNAME: 1, PRODUCT_ID_VAL: 1},
                    "name": format!("{}-{}",PRODUCT_ID_FIELDNAME,PRODUCT_ID_VAL),
                    "unique": "true"
                    }),
        Bson::Document( doc! { "key": {SUBMITTER_ID: 1}, "name":SUBMITTER_ID })]);

    mongo::update_indexes(db, CRYSTAL_METADATA_MUTEX_COLL_NAME, indexes)?;

    Ok(())
}

pub fn unlock_all(db: &Database) -> io::Result<()> {
    let cmd = doc!{
        "delete": CRYSTAL_METADATA_MUTEX_COLL_NAME,
        "deletes": Bson::Array(vec![Bson::Document(doc!{
            "q":{SUBMITTER_ID:core::get_my_id()?},
            "limit":0
            })])
        };
    db.command_simple(cmd.clone(), None)
        .map_err(|err| Error::new(ErrorKind::Other, format!("Cannot delete mutexes: {} cmd: {:?}",err,cmd)))?;
    Ok(())
}