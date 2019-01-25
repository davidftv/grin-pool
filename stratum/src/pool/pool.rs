// Copyright 2018 Blade M. Doyle
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use bufstream::BufStream;
use std::collections::HashMap;
use std::net::{SocketAddr, TcpListener};
use std::sync::{Arc, Mutex}; //, RwLock
use std::time::Instant;
use std::{thread, time};
use std::env;

//use pool::config::{Config, NodeConfig, PoolConfig, WorkerConfig};
use pool::config::{Config};
use pool::logger::LOGGER;
use pool::proto::{JobTemplate, RpcError, SubmitParams};
use pool::server::Server;
use pool::worker::Worker;
//use std::time::Duration;

use r2d2_redis::redis::{Commands,PubSubCommands, ControlFlow};

use r2d2_redis::{r2d2, RedisConnectionManager};
use r2d2_redis::r2d2::Pool as RDPool;
use r2d2_redis::r2d2::ManageConnection;
use redis::FromRedisValue;
//use redis::{PubSubCommands, ControlFlow};

use std::io;


//use simple_server;
// ----------------------------------------
// Worker Connection Thread Function
// Run in a thread. Adds new connections to the workers list
fn typeid<T: std::any::Any>(_: &T) {
    println!("{:?}", std::any::TypeId::of::<T>());
}

fn accept_workers(
    id: String,
    address: String,
    difficulty: u64,
    workers: &mut Arc<Mutex<Vec<Worker>>>,
    redpool: &mut RDPool<RedisConnectionManager>,
) {
    let listener = TcpListener::bind(address).expect("Failed to bind to listen address");
    let mut worker_id: usize = 0;
    let banned: HashMap<SocketAddr, Instant> = HashMap::new();
    // XXX TODO: Call the pool-api to get a list of banned IPs, refresh that list sometimes
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                match stream.peer_addr() {
                    Ok(worker_addr) => {
                        stream
                            .set_nonblocking(true)
                            .expect("set_nonblocking call failed");
                        let mut worker = Worker::new(worker_id, BufStream::new(stream));
                        worker.set_difficulty(difficulty);
                        worker.setRedPool(Some(redpool.clone()));
                        worker.clientip = Some(worker_addr.to_string());
                        workers.lock().unwrap().push(worker);
                        worker_id = worker_id + 1;
                    }
                    Err(e) => {
                        warn!(
                            LOGGER,
                            "{} - Worker Listener - Error getting wokers ip address: {:?}", id, e
                        );
                    }
                }
            }
            Err(e) => {
                warn!(
                    LOGGER,
                    "{} - Worker Listener - Error accepting connection: {:?}", id, e
                );
            }
        }
    }
    // close the socket server
    drop(listener);
}

// ----------------------------------------
// A Grin mining pool

pub struct Pool {
    id: String,
    job: JobTemplate,
    config: Config,
    server: Server,
    workers: Arc<Mutex<Vec<Worker>>>,
    duplicates: HashMap<Vec<u32>, usize>, // pow vector, worker id who first submitted it
    redispool: Option<RDPool<RedisConnectionManager>>,
}

impl Pool {
    /// Create a new Grin Stratum Pool
    pub fn new(config: Config) -> Pool {
        Pool {
            id: "Grin Pool".to_string(),
            job: JobTemplate::new(),
            config: config.clone(),
            server: Server::new(config.clone()),
            workers: Arc::new(Mutex::new(Vec::new())),
            duplicates: HashMap::new(),
            redispool: None,
        }
    }

    /// Run the Pool
    pub fn run(&mut self) {
        // Start a thread for each listen port to accept new worker connections


        let redis_host = self.config.workers.redis_host.clone();
        let redis_port = self.config.workers.redis_port;
        let redis_db = self.config.workers.redis_db;

        let redis_stats = format!("redis://{}:{}/{}", redis_host,redis_port,redis_db);

        println!("{:?}",redis_stats);

        //let client = Client::open("redis://127.0.0.1/").unwrap();
        let manager = RedisConnectionManager::new(redis_stats.as_str()).unwrap();
        let pool = r2d2::Pool::builder().build(manager).unwrap();
        self.redispool = Some(pool.clone());
        self.server.redpool =  Some(pool.clone());

        //let mut server_workers = self.workers.clone();
        // thread::spawn(move || {
        //     self.server.run();
        // });

        let port_len = self.config.workers.port_len;
        let port_start = self.config.workers.port_start;

        let diff_start = self.config.workers.diff_start;
        let diff_coef = self.config.workers.diff_coef;

        for i in 0..port_len {
            let mut redpool = pool.clone();
            let mut workers_th = self.workers.clone();
            let id_th = self.id.clone();
            let port = port_start+i*2;
            let address_th = format!("{}:{}", self.config.workers.listen_address.clone(),port);

            let difficulty_th = diff_start*diff_coef;
            let _listener_th = thread::spawn(move || {
                accept_workers(id_th, address_th, difficulty_th, &mut workers_th,&mut redpool);
            });
        }


        // let apiport = match env::var("api.port") {
        //     Ok(val) => val,
        //     Err(_) => "9888".to_string(),
        // };


        let apiport = self.config.workers.pool_api_port;

        let api_server_addr = format!("{}:{}", "0.0.0.0",apiport);
        let wks2 = self.workers.clone();
        thread::spawn(move|| {
            rouille::start_server(&api_server_addr, move |request| {
                rouille::log(&request, io::stdout(), || {
                    rouille::router!(request,
                        (GET) (/allminer) => {
                            let mut workers_l = wks2.lock().unwrap();
                            //let mut return_vec = vec![];
                            let mut sbuf = String::from("");
                            for worker in workers_l.iter_mut() {
                                let id = worker.id;
                                let user = worker.login();
                                let status = &worker.status;
                                //return_vec.push((id,user,status));
                                sbuf.push_str(user.as_str());
                                sbuf.push_str(",");
                                sbuf.push_str(serde_json::to_string(status).unwrap().as_str());
                                sbuf.push_str(" - <br>\n");
                            }
                            //let serialized = serde_json::to_string(&return_vec).unwrap();
                            // When viewing the home page, we return an HTML document described below.
                            rouille::Response::html(sbuf)
                        },
                        (GET) (/user/{id: String}) => {

                            let mut workers_l = wks2.lock().unwrap();
                            // let mut return_vec = vec![];
                            let mut sbuf = String::from("");
                            for worker in workers_l.iter_mut() {
                                let user = worker.login();
                                if user.contains(&id) {
                                    let id = worker.id;
                                    let status = &worker.status;
                                    // return_vec.push((id,user,status));
                                    sbuf.push_str(user.as_str());
                                    sbuf.push_str(",");
                                    sbuf.push_str(serde_json::to_string(status).unwrap().as_str());
                                    sbuf.push_str(" - <br>\n");
                                }
                            }
                            //let serialized = serde_json::to_string(&return_vec).unwrap();
                            rouille::Response::html(sbuf)
                        },
                        (POST) (/submit) => {

                            let data = try_or_400!(post_input!(request, {
                                txt: String,
                                files: Vec<rouille::input::post::BufferedFile>,
                            }));

                            println!("Received data: {:?}", data);

                            rouille::Response::html("Success! <a href=\"/\">Go back</a>.")
                        },

                        _ => rouille::Response::empty_404()
                    )
                })
            });
        });

        let wks3 = self.workers.clone();
        let rpool1 = self.redispool.clone();
        let curJob = self.job.clone();
        thread::spawn(move|| {
            let mut conn = rpool1.unwrap().get().unwrap();
            // let mut pubsub = conn.as_pubsub();
            // pubsub.subscribe("jobs:grin");
            let mut count = 0;
            loop {
                conn.subscribe(&["jobs:grin"], |msg| {
                    let ch = msg.get_channel_name();
                    count += 1;
                    let payload: String = msg.get_payload().unwrap();
                    match payload.as_ref() {
                        "10" => ControlFlow::Break(()),
                        a => {
                            let mut workers_l = wks3.lock().unwrap();
                            println!("Channel '{}' received '{}'.", ch, a);
                            let mut job: JobTemplate = serde_json::from_str(a).unwrap();
                            if curJob.pre_pow != job.pre_pow || job.height>curJob.height {
                                //self.job = job.clone();
                                for worker in workers_l.iter_mut() {
                                    if worker.needs_job {
                                        worker.send_job(&mut job.clone());
                                    }
                                }
                            }else{
                                println!("dup: Channel '{}' received '{}'.", ch, a);
                            }
                            ControlFlow::Continue
                        }
                    }
                }).unwrap();
                thread::sleep(time::Duration::from_millis(50));
            }
        });
        // ------------
        // Main loop
        loop {
            match self.server.connect() {
                Ok(_) => {}
                Err(e) => {
                    error!(
                        LOGGER,
                        "{} - Unable to connect to upstream server: {}", self.id, e
                    );
                    thread::sleep(time::Duration::from_secs(1));
                    continue;
                }
            }

            // check the server for messages and handle them
            let _ = self.process_server_messages();

            // if the server gave us a new block
            let _ = self.accept_new_job();

            // Process messages from the workers
            let _ = self.process_worker_messages();

            // Process worker shares
            let _ = self.process_shares();

            // Send jobs to needy workers
            //let _ = self.send_jobs();

            // Delete workers in error state
            let _num_active_workers = self.clean_workers();

            thread::sleep(time::Duration::from_millis(50));
        }
    }

    // ------------
    // Pool Methods
    //

    // Process messages from the upstream server
    // Will contain job requests, submit results, status results, etc...
    fn process_server_messages(&mut self) -> Result<(), RpcError> {
        match self.server.process_messages(&mut self.workers) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                // In most cases just log an error and continue
                error!(
                    LOGGER,
                    "{} - Error processing upstream message: {:?}", self.id, e
                );
                // There are also special case(s) where we want to do something for a specific
                // error
                if e.message.contains("Node is syncing") {
                    thread::sleep(time::Duration::from_secs(2));
                }
                return Err(e);
            }
        }
    }

    fn process_worker_messages(&mut self) {
        let mut workers_l = self.workers.lock().unwrap();
        for worker in workers_l.iter_mut() {
            let _ = worker.process_messages();
        }
    }

    fn send_jobs(&mut self) {
        let mut workers_l = self.workers.lock().unwrap();
        for worker in workers_l.iter_mut() {
            if worker.needs_job {
                // Randomize the nonce
                // XXX TODO (Need to know block header format and deserialize it
                worker.send_job(&mut self.server.job.clone());
            }
        }
    }

    fn accept_new_job(&mut self) {
        if self.job.pre_pow != self.server.job.pre_pow {
            // Use the new job
            self.job = self.server.job.clone();
            // broadcast it to the workers
            let _ = self.broadcast_job();
            // clear last block duplicates map
            self.duplicates = HashMap::new()
        }
    }

    fn find_share_difficulty(&mut self, share: SubmitParams) -> u64 {
        // https://github.com/mimblewimble/grin/blob/2fa32d15ce5031aff9d3366b49c3aaca40adbdce/chain/src/pipe.rs#L280
        // XXX TODO: This
        // if header.pow.to_difficulty() < target_difficulty {
        //	return Err(Error::DifficultyTooLow);
        // }
        // XXX Currently share difficulty is checked by the grin stratum server, and logged in the grin.log
        return 0;
    }

    //
    // Process shares returned by each workers
    fn process_shares(&mut self) {
        let mut workers_l = self.workers.lock().unwrap();
        for worker in workers_l.iter_mut() {
            match worker.get_shares().unwrap() {
                None => {}
                Some(shares) => {
                    for share in shares {
                        //  Check for duplicate or add to duplicate map
                        if self.duplicates.contains_key(&share.pow) {
                            debug!(
                                LOGGER,
                                "{} - Rejected duplicate share from worker {} with login {}",
                                self.id,
                                worker.id(),
                                worker.login(),
                            );
                            worker.status.rejected += 1;
                            worker.block_status.rejected += 1;
                            continue; // Dont process this share anymore
                        } else {
                            self.duplicates.insert(share.pow.clone(), worker.id());
                        }
                        // XXX TODO:
                        // Verify the timestamp matches what we sent so we know
                        //   this share comes from the job we sent
                        // XXX TO DO This I need to deserialize the block header
                        //			if share.pre_pow != self.current_block {
                        //			    debug!(
                        //                                LOGGER,
                        //                                "{} - Rejected corrupt share from worker {} with login {}",
                        //                                self.id,
                        //                                worker.id(),
                        //                                worker.login(),
                        //                            );
                        //                            worker.status.rejected += 1;
                        //                            worker.block_status.rejected += 1;
                        //                            continue; // Dont process this share anymore
                        //                        }
                        // We dont know the difficulty so we cant check that here
                        // Send it to the upstream server for further verification and logging
                        self.server.submit_share(&share.clone(), worker.id());
                        warn!(LOGGER, "{} - Got share at height {} with nonce {} with difficulty {} from worker {}",
                                self.id,
                                share.height,
                                share.nonce,
                                worker.status.difficulty,
                                worker.id,
                        );
                    }
                }
            }
        }
    }

    fn broadcast_job(&mut self) -> Result<(), String> {
        let mut workers_l = self.workers.lock().unwrap();
        debug!(
            LOGGER,
            "{} - broadcasting a job to {} workers",
            self.id,
            workers_l.len()
        );
        // XXX TODO: To do this I need to deserialize the block header
        // XXX TODO: need to randomize the nonce (just in case a miner forgets)
        // XXX TODO: need to set a unique timestamp and record it in the worker struct
        for num in 0..workers_l.len() {
            workers_l[num].set_difficulty(1); // XXX TODO: this get from config?
            workers_l[num].set_height(self.job.height);
            workers_l[num].send_job(&mut self.job.clone());
        }
        return Ok(());
    }

    // Purge dead/sick workers - remove all workers marked in error state
    fn clean_workers(&mut self) -> usize {
        let mut start = 0;
        let mut workers_l = self.workers.lock().unwrap();
        loop {
            for num in start..workers_l.len() {
                if workers_l[num].error() == true {
                    warn!(
                        LOGGER,
                        "{} - Dropping worker: {}",
                        self.id,
                        workers_l[num].id()
                    );
                    // Remove the dead worker
                    workers_l.remove(num);
                    break;
                }
                start = num + 1;
            }
            if start >= workers_l.len() {
                return workers_l.len();
            }
        }
    }
}
