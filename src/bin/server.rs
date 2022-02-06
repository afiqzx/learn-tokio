use mini_redis::{Connection, Frame};
use tokio::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use bytes::Bytes;

type Db = Arc<Mutex<HashMap<String, Bytes>>>;
type ShardedDb = Arc<Vec<Mutex<HashMap<String, Vec<u8>>>>>;

#[tokio::main]
async fn main() {
    // bind listener
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    println!("Listening");

    // wrap the Hashmap database inside an mutexed arc
    let db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        // The second item contains th IP and port of the new connection;
        let (socket, _) = listener.accept().await.unwrap();
        
        let db = db.clone();

        println!("Accepted");
        tokio::spawn(async move {
            process(socket, db).await;
        });
    }
}

async fn process(socket: TcpStream, db: Db) {
    use mini_redis::Command::{self, Get, Set};

    // The Connection lets us read/write redis **frames** instead of
    // byte streams. The Connection type is defined by min-redis
    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db = db.lock().unwrap();
                // the valuue is stored as Vec<u8>
                db.insert(cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let db = db.lock().unwrap();


                if let Some(value) = db.get(cmd.key()) {
                    Frame::Bulk(value.clone().into())
                } else {
                    Frame::Null
                }
            }

            cmd => panic!("unimplemented {:?}", cmd),

        };

        // Write the response to the client 
        connection.write_frame(&response).await.unwrap();
    }
}

fn new_sharded_db(num_shards: usize) -> ShardedDb {
    let mut db = Vec::with_capacity(num_shards);

    for _ in 0..num_shards {
        db.push(Mutex::new(HashMap::new()));
    }

    Arc::new(db)
}


// demo for mutex guard 
#[allow(dead_code)]
struct CanIncrement {
    mutex: Mutex<i32>,
}

#[allow(dead_code)]
impl CanIncrement {
    // This fuunction is not marked async
    fn increment(&self) {
        let mut lock = self.mutex.lock().unwrap();
        *lock += 1;
    }
}

// this is just for demo
#[allow(dead_code)]
async fn increment_and_do_stuff(can_incr: &CanIncrement) {
    can_incr.increment();
    //do_something_async().await;
}
