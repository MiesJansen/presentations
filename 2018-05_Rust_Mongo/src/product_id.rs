use std::io::{self, Error, ErrorKind};
use mongo_driver::database::Database;
use product::{PRODUCT_COLL_NAME};

pub const CRYSTAL_PRODUCT_INTERNAL_ID_KEY: &'static str = "_crystalProductId";

type CrystalProductId = String;
enum CrystalProductIdState {
    New,
    Existing,
}

fn get_from_product_table(db: &Database, id_field_name: &str, id_val: &str) -> io::Result<Option<CrystalProductId>>{
    let cmd = doc! {
        "find": PRODUCT_COLL_NAME,
        "filter": doc! {
            id_field_name: id_val
        }
    };
    let doc_opt = db.command_batch(cmd, None)
        .map_err(|err|
            Error::new(
                ErrorKind::Other,
                format!("cannot read product id from db: {}  cmd: {:?}",err, cmd)
            )
        )?.next();

    if let Some(doc) = doc_opt {
        let c_id = doc
            .map_err(|err|
                Error::new(
                    ErrorKind::Other,
                    format!("cannot read result from db: {}  cmd: {:?}",err, cmd)
                )
            )?
            .get_str(CRYSTAL_PRODUCT_INTERNAL_ID_KEY)
            .map_err(|err|
                Error::new(
                    ErrorKind::Other,
                    format!("cannot find crystal product id: {}  cmd: {:?}",err, cmd)
                )
            )?;
        Ok(Some(c_id.to_owned()))
    } else {
        Ok(None)
    }
}

pub fn get(db: &Database, external_id_fieldname: &str, external_id_val: &str) -> io::Result<(CrystalProductId,CrystalProductIdState)>{
    //ensure no duplicate ids
    // * check the product table
    let c_id_opt = get_from_product_table(db, external_id_fieldname, external_id_val)?;
    if let Some(c_id) = c_id_opt {
        //   * if it's there
        Ok((c_id,CrystalProductIdState::New))
    } else {
        //   * if it's not there...
        //      * Add it!
        //      * lock it
        //          * if you can't then get it from the mutex and add it
        //      * check again that's not in the product table
        //      * Add it to queue and only in this case remove the lock after the bulk add is complete!
    }

}