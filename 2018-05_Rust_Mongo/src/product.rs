use std::io::{self,Error,ErrorKind};
use mongo_driver::database::Database;
use csv;
use bson::{Bson,Document};
use product_id::{self, CRYSTAL_PRODUCT_INTERNAL_ID_KEY};

const BULK_WRITE_SIZE: usize = 1000;
const EXTERNAL_PRODUCT_ID_KEY: &'static str = "bond_id";
pub const PRODUCT_COLL_NAME: &'static str = "products";

fn bulk_write(docs: Vec<Document>) {
    // write docs
    // clear my mutexes
}

pub fn put <R: io::Read> (
    product_data_stream: R,
    db: &Database) -> io::Result<String> {

    //send csv to product db
    let mut rdr = csv::ReaderBuilder::new().trim(csv::Trim::All).from_reader(product_data_stream);
    let headers = rdr.headers()?.clone();
    let mut rec_count: usize = 0;

    for record in rdr.records() {
        let mut doc = Document::new();  // TODO replace w. serde

        for (key, val) in headers.iter().zip(record?.iter()) {
            doc.insert_bson(key.to_owned(), Bson::String(val.to_owned()));
        }

        let (c_id, is_new) = product_id::get(
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

        println!("{:?}", doc);

        rec_count += 1;
    }


    //tell UI to update

    //Err(Error::new(ErrorKind::Other, format!("not done")))
    Ok(format!("records processed: {}", rec_count))
}

