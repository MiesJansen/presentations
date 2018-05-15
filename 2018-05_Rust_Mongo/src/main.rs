#![feature(plugin)]
#![plugin(rocket_codegen)]

// https://rocket.rs/guide/getting-started/
extern crate rocket;
extern crate csv;
extern crate mongo_driver;
#[macro_use(bson, doc)]
extern crate bson;

// error! conflicts with rocket
use rocket::request::{FromRequest};
use rocket::{Data, Request, State, Outcome};
use rocket::http::{Status};

mod product;
mod mongo;
mod product_id;

use std::io::{self, Read, ErrorKind};


impl<'a, 'r> FromRequest<'a, 'r> for mongo::DbConn<'r> {
    type Error = String;

    fn from_request(request: &'a Request<'r>) -> Outcome<mongo::DbConn<'r>, (Status,Self::Error), ()> {
        let pool_state = request.guard::<State<mongo::Pool>>()
            .map_failure(|(status,_)|(status,"Cannot make connection to db".to_owned()))?;
        Outcome::Success(mongo::DbConn(pool_state.inner().pop()))
    }
}

fn return_error_to_client(res: io::Result<String>) -> io::Result<String> {
    if let Err(r) = res {
        Ok(format!("Error: {:?}",r))
    }
        else {
            res
        }
}

fn return_error_to_client_clear_stream<R: io::Read>(mut stream: R, res: io::Result<String>) -> io::Result<String> {
    if res.is_err() && ErrorKind::UnexpectedEof == res.as_ref().err().expect("return_error_to_client_clear_stream: said we had an error, but none found").kind() {
        return res;
    }
    // means the entire stream needs to be transfered before seeing an error -- not good
    for _byte in stream.by_ref().bytes() { }
    return_error_to_client(res)
}

#[post("/product_data/put", data = "<product_data>")]
fn upload_product(product_data: Data, conn: mongo::DbConn) -> io::Result<String> {
    let mut stream = product_data.open();
    let res = product::put(stream.by_ref(), &mongo::get_db(&conn));
    return_error_to_client_clear_stream(stream.by_ref(),res)
}

fn db_ignite() -> rocket::Rocket {
    rocket::ignite()
        .manage(mongo::init_pool())

}

fn rocket() -> rocket::Rocket {
    db_ignite()
        .mount("/",
               routes![
                    upload_product]
        )
}

// upgrade to latest Rust so we can return Result :)
fn main() {
    rocket().launch();
}
