use std::io::{self,Error,ErrorKind};
use mongo_driver::{collection, write_concern};
use mongo_driver::database::Database;
use csv;
use bson::{Bson,Document};
use product_id::{self, CRYSTAL_PRODUCT_INTERNAL_ID_KEY, CrystalProductIdState};
use core;

const BULK_WRITE_SIZE: usize = 1000;
const EXTERNAL_PRODUCT_ID_KEY: &'static str = "bond";
pub const PRODUCT_COLL_NAME: &'static str = "products";
pub const AS_OF_TIME: &'static str = "_asOfUtc";
pub const SUBMISSION_NAME: &'static str = "submission_name";

fn bulk_write(db: &Database, docs: &mut Vec<Document>, id_state: CrystalProductIdState) -> io::Result<()> {
    let coll = db.get_collection(PRODUCT_COLL_NAME);
    let bulk_op = coll.create_bulk_operation(Some(&collection::BulkOperationOptions{ordered: false, write_concern: write_concern::WriteConcern::default()}));
    for document in docs.iter() {
        if let Err(err) = bulk_op.insert(document) {
            return Err(Error::new(ErrorKind::Other, format!("cannot add product to insertion queue: {}, product: {:?}",err, document)));
        }
    }
    let result_doc = bulk_op.execute()
        .map_err((|ref err| Error::new(ErrorKind::Other, format!("cannot write products to db: {}, products: {:?}",err, docs))))?;
    println!("Bulk Insert Results: {:?}", result_doc);

    if id_state == CrystalProductIdState::ClearMutex { product_id::unlock_all(db)?; }
    docs.clone_from(&Vec::with_capacity(BULK_WRITE_SIZE)); // erase!!

    Ok(())
}

pub fn put <R: io::Read> (
    product_data_stream: R,
    db: &Database) -> io::Result<String> {

    product_id::ensure_mutex_indicies(db)?; // move to init for production

    //send csv to product db
    let mut rdr = csv::ReaderBuilder::new().trim(csv::Trim::All).from_reader(product_data_stream);
    let headers = rdr.headers()?.clone();
    let mut rec_count: usize = 0;
    let mut queue: Vec<Document> = Vec::with_capacity(BULK_WRITE_SIZE);
    let mut is_new = CrystalProductIdState::Existing;
    let my_id = core::get_my_id()?;

    for record in rdr.records() {
        let mut doc = Document::new();

        doc.insert(SUBMISSION_NAME,format!("{}-{}",core::yyyymm_ddhhss(),my_id));
        doc.insert_bson(AS_OF_TIME.to_owned(),Bson::I64(core::timestamp_ms()? as i64));

        for (key, val) in headers.iter().zip(record?.iter()) {
            doc.insert_bson(key.to_owned(), Bson::String(val.to_owned()));
        }

        let (c_id, is_new_curr) = product_id::get(
            db,
            EXTERNAL_PRODUCT_ID_KEY,
            doc.get_str(EXTERNAL_PRODUCT_ID_KEY)
                .map_err(|err|
                    Error::new(
                        ErrorKind::Other,
                        format!("must supply an external product id: {}",err)
                    )
                )?
        )?;
        doc.insert_bson(CRYSTAL_PRODUCT_INTERNAL_ID_KEY.to_owned(),
                        Bson::String(c_id));

        if is_new_curr == CrystalProductIdState::ClearMutex {is_new = CrystalProductIdState::ClearMutex;}
        queue.push(doc.clone());
        //println!("{:?}", doc);
        rec_count += 1;
        if queue.len() == BULK_WRITE_SIZE {
            bulk_write(db, &mut queue, is_new )?;
            is_new = CrystalProductIdState::Existing;
        }
    }
    if queue.len() > 0 {
        bulk_write(db, &mut queue, is_new)?;
    }

    //tell UI to update

    Ok(format!("records processed: {}", rec_count))
}

