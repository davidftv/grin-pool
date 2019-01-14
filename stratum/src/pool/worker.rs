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

//! Mining Stratum Worker
//!
//! A single mining worker (the pool manages a vec of Workers)
//!
use redis::{Client, Commands, Connection, RedisResult};
use r2d2_redis::{r2d2, redis, RedisConnectionManager};
use r2d2_redis::r2d2::Pool;
use r2d2_redis::r2d2::ManageConnection;
use bufstream::BufStream;
use serde_json;
use serde_json::Value;
use std::net::TcpStream;
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime};
use chrono::prelude::*;

use pool::logger::LOGGER;
use pool::proto::{JobTemplate, LoginParams, StratumProtocol, SubmitParams, WorkerStatus,IncrementType};
use reqwest;
use std::collections::HashMap;

use pool::proto::{RpcRequest, RpcError};

// ----------------------------------------
// Worker Object - a connected stratum client - a miner
//

//#derive(Serialize, Deserialize, Debug)]
//pub struct AuthForm {
//    pub username: String,
//    pub password: String,
//}

#[derive(Debug)]
pub struct WorkerConfig {}

pub struct Worker {
    pub id: usize,
    pub login: Option<LoginParams>,
    stream: BufStream<TcpStream>,
    protocol: StratumProtocol,
    error: bool,
    authenticated: bool,
    pub status: WorkerStatus,       // Runing totals
    pub block_status: WorkerStatus, // Totals for current block
    pub shares: Vec<SubmitParams>,
    pub needs_job: bool,
    pub redpool: Option<Pool<RedisConnectionManager>>,
    pub redpool2: Option<Pool<RedisConnectionManager>>,
}

impl Worker {
    /// Creates a new Stratum Worker.
    pub fn new(id: usize, stream: BufStream<TcpStream>) -> Worker {
        Worker {
            id: id,
            login: None,
            stream: stream,
            protocol: StratumProtocol::new(),
            error: false,
            authenticated: false,
            status: WorkerStatus::new(id.to_string()),
            block_status: WorkerStatus::new(id.to_string()),
            shares: Vec::new(),
            needs_job: true,
            redpool: None,
            redpool2: None,
        }
    }

    pub fn setRedPool(&mut self, p: Option<Pool<RedisConnectionManager>>){
        self.redpool = p;
    }
    pub fn setRedPool2(&mut self, p: Option<Pool<RedisConnectionManager>>){
       self.redpool2 = p;
    }

    /// Is the worker in error state?
    pub fn error(&self) -> bool {
        return self.error;
    }

    /// get the id
    pub fn id(&self) -> usize {
        return self.id;
    }

    /// Get worker login
    pub fn login(&self) -> String {
        match self.login {
            None => "None".to_string(),
            Some(ref login) => {
                let mut loginstr = login.login.clone();
                return loginstr.to_string();
            }
        }
    }
    fn parse(&self,input: &str) -> (String,String) {
        if input.is_empty() {
            return ("mineros".to_string(),"noworkid1".to_string())
        }
        let parts: Vec<&str> = input.rsplitn(2, '.').collect();

        if parts.len() < 2 {
            return (parts[0].to_string(),"noworkid2".to_string())
        }

        (parts[1].to_string(),parts[0].to_string())
    }
    pub fn getUserAndWorkId(&self) -> (String,String) {
        match self.login {
            None => return ("mineros".to_string(),"noworkid".to_string()),
            Some(ref login) => {
                let mut loginstr = login.login.clone();
                return self.parse(&loginstr);
            }
        }
        ("mineros".to_string(),"noworkid".to_string())
    }
    pub fn incrCounter(&mut self,ctType: IncrementType) {
        // let s = match ctType {
        //     IncrementType::AC => {self.status.accepted=self.status.accepted+0;"accept"},
        //     IncrementType::RE => {self.status.rejected=self.status.rejected+0;"rejected"},
        //     IncrementType::ST => {self.status.stale=self.status.stale+0;"stale"},
        //     IncrementType::BF => {self.status.blockfound=self.status.blockfound+0;"blockfound"},
        // };
        let s = match ctType {
            IncrementType::AC => "accept",
            IncrementType::RE => "rejected",
            IncrementType::ST => "stale",
            IncrementType::BF => "blockfound",
        };

        let dt = Utc::today().format("%Y-%m-%d");

        // let client = Client::open("redis://127.0.0.1/").unwrap();
        // let conn = client.get_connection().unwrap();
        // let now: DateTime<Utc> = Utc::now();
        // let curtime = now.format("%a %b %e %T %Y");
        // fmt::format("grin:{}:{}:{}",)
        // let _: () = conn.set(formatted_number, self.status.accepted).unwrap();



        let (username,workid) = self.getUserAndWorkId();
        trace!(LOGGER, "Worker user info {:?}",(&username,&workid));


        let conn = self.redpool.clone().unwrap().get().unwrap();

        //daily total value
        let daily_total = format!("grin:{}:{}",dt,s);
        let _: () = conn.incr(daily_total,1).unwrap();

        //user daily total
        let daily_user = format!("grin:{}:{}:{}", dt,username,s);
        let _: () = conn.incr(daily_user,1).unwrap();

        //user daily miner total
        // let daily_user = format!("grin:{}:{}:{}:{}", dt,username,workid,s);
        // let _: () = conn.incr(daily_user,1).unwrap();

        //by user  daily total
        let user_day_total = format!("grin:{}:{}:{}", username,dt,s);
        let _: () = conn.incr(user_day_total,1).unwrap();

        //by user daily miner  total
        let user_day_total = format!("grin:{}:{}:{}:{}", username,dt,workid,s);
        let _: () = conn.incr(user_day_total,1).unwrap();

    }

    pub fn addBlock(&mut self,block: &String) {
        // let client = Client::open("redis://127.0.0.1/8").unwrap();
        // let conn = client.get_connection().unwrap();
        //
        let conn = self.redpool2.clone().unwrap().get().unwrap();

        let dt = Utc::today().format("%Y-%m-%d");
        let daily_user = format!("grin:{}:{}:blocks",dt,self.login().clone());
        let _: () = conn.hset(daily_user,self.status.height,block).unwrap();

    }
    pub fn incrAccept(&mut self){
        let t = IncrementType::AC;
        self.incrCounter(t);
    }
    pub fn incrReject(&mut self){
        let t = IncrementType::RE;
        self.incrCounter(t);
    }
    pub fn incrStale(&mut self){
        let t = IncrementType::ST;
        self.incrCounter(t);
    }
    pub fn incrBlockFound(&mut self){
        let t = IncrementType::BF;
        self.incrCounter(t);
    }

    /// Set job difficulty
    pub fn set_difficulty(&mut self, new_difficulty: u64) {
        self.status.difficulty = new_difficulty;
    }

    /// Set job height
    pub fn set_height(&mut self, new_height: u64) {
        self.status.height = new_height;
    }

    // XXX TODO: I may need seprate send_job_request() and send_jog_response() methods?
    /// Send a job to the worker
    pub fn send_job(&mut self, job: &mut JobTemplate) -> Result<(), String> {
        trace!(LOGGER, "Worker {} - Sending a job downstream", self.id);
        // Set the difficulty
        job.difficulty = self.status.difficulty;
        self.needs_job = false;
        let job_value = serde_json::to_value(job).unwrap();
        return self.protocol.send_response(
            &mut self.stream,
            "getjobtemplate".to_string(),
            job_value,
            self.id,
        );
    }

    /// Send worker mining status
    pub fn send_status(&mut self, status: WorkerStatus) -> Result<(), String> {
        trace!(LOGGER, "Worker {} - Sending worker status", self.id);
        let status_value = serde_json::to_value(status).unwrap();
        return self.protocol.send_response(
            &mut self.stream,
            "status".to_string(),
            status_value,
            self.id,
        );
    }

    /// Send OK Response
    pub fn send_ok(&mut self, method: String) -> Result<(), String> {
        trace!(LOGGER, "Worker {} - sending OK Response", self.id);
        return self.protocol.send_response(
            &mut self.stream,
            method.to_string(),
            serde_json::to_value("ok".to_string()).unwrap(),
            self.id,
        );
    }
    /// Send reject Response
    pub fn send_reject(&mut self, method: String,res: String) -> Result<(), String> {
        trace!(LOGGER, "Worker {} - sending OK Response", self.id);
        return self.protocol.send_response(
            &mut self.stream,
            method,
            serde_json::to_value(res).unwrap(),
            self.id,
        );
    }

    /// Send Err Response
    pub fn send_err(&mut self, method: String, message: String, code: i32) -> Result<(), String> {
        trace!(LOGGER, "Worker {} - sending Err Response", self.id);
        let e = RpcError {
            code: code,
            message: message.to_string(),
        };
        return self.protocol.send_error_response(
            &mut self.stream,
            method.to_string(),
            e,
        );
    }
    /// Return any pending shares from this worker
    pub fn get_shares(&mut self) -> Result<Option<Vec<SubmitParams>>, String> {
        if self.shares.len() > 0 {
            trace!(
                LOGGER,
                "Worker {} - Getting {} shares",
                self.id,
                self.shares.len()
            );
            let current_shares = self.shares.clone();
            self.shares = Vec::new();
            return Ok(Some(current_shares));
        }
        return Ok(None);
    }

    /// Get and process messages from the connected worker
    // Method to handle requests from the downstream worker
    pub fn process_messages(&mut self) -> Result<(), String> {
        // XXX TODO: With some reasonable rate limiting (like N message per pass)
        // Read some messages from the upstream
        // Handle each request
        match self.protocol.get_message(&mut self.stream) {
            Ok(rpc_msg) => {
                match rpc_msg {
                    Some(message) => {
                        trace!(LOGGER, "Worker {} - Got Message: {:?}", self.id, message);
                        // let v: Value = serde_json::from_str(&message).unwrap();
                        let req: RpcRequest = match serde_json::from_str(&message) {
                            Ok(r) => r,
                            Err(e) => {
                                self.error = true;
                                debug!(LOGGER, "Worker {} - Got Invalid Message", self.id);
                                // XXX TODO: Invalid request
                                return Err(e.to_string());
                            }
                        };
                        trace!(
                            LOGGER,
                            "Worker {} - Received request type: {}",
                            self.id,
                            req.method
                        );
                        match req.method.as_str() {
                            "login" => {
                                debug!(LOGGER, "Worker {} - Accepting Login request", self.id);
                                let params: Value = match req.params {
                                    Some(p) => p,
                                    None => {
                                        self.error = true;
                                        debug!(LOGGER, "Worker {} - Missing Login request parameters", self.id);
                                        return self.send_err(
                                            "login".to_string(),
                                            "Missing Login request parameters".to_string(),
                                            -32500,
                                        );
                                        // XXX TODO: Invalid request
                                        //return Err("Invalid Login request".to_string());
                                    }
                                };
                                let login_params: LoginParams = match serde_json::from_value(params)
                                {
                                    Ok(p) => p,
                                    Err(e) => {
                                        self.error = true;
                                        debug!(LOGGER, "Worker {} - Invalid Login request parameters", self.id);
                                        return self.send_err(
                                            "login".to_string(),
                                            "Invalid Login request parameters".to_string(),
                                            -32500,
                                        );
                                        // XXX TODO: Invalid request
                                        //return Err(e.to_string());
                                    }
                                };
                                // We accepted the login, send ok result
                                debug!(LOGGER, "assign worker[{:?}] to {:?}", self.id,login_params);
                                self.login = Some(login_params);
                                self.send_ok(req.method);
                            }
                            "getjobtemplate" => {
                                debug!(LOGGER, "Worker {} - Accepting request for job", self.id);
                                self.needs_job = true;
                            }
                            "submit" => {
                                debug!(LOGGER, "Worker {} - Accepting share", self.id);
                                match serde_json::from_value(req.params.unwrap()) {
					Result::Ok(share) => {
                           			self.shares.push(share);
					},
					Result::Err(err) => { }
				};

                            }
                            "status" => {
                                trace!(LOGGER, "Worker {} - Accepting status request", self.id);
                                let status = self.status.clone();
                                self.send_status(status);
                            }
                            "keepalive" => {
                                trace!(LOGGER, "Worker {} - Accepting keepalive request", self.id);
                                self.send_ok(req.method);
                            }
                            _ => {
                                warn!(
                                    LOGGER,
                                    "Worker {} - Unknown request: {}",
                                    self.id,
                                    req.method.as_str()
                                );
                                self.error = true;
                                return Err("Unknown request".to_string());
                            }
                        };
                    }
                    None => {} // Not an error, just no messages for us right now
                }
            }
            Err(e) => {
                self.error = true;
                return Err(e.to_string());
            }
        }
        return Ok(());
    }
}
