// Run me with "cargo run --example 24x7".
// Or "cargo run --example 24x7 -- --pin=0"

#![feature(type_ascription, integer_atomics)]

#[macro_use] extern crate fomat_macros;
extern crate futures;
extern crate futures_cpupool;
extern crate gstuff;
extern crate pg_async;
extern crate rand;

use futures::future::Future;
use futures_cpupool::CpuPool;
use gstuff::{status_line, ISATTY};
use pg_async::{Cluster, PgOperation, PgSchedulingMode};
use pg_async::PgQueryPiece::{Plain as P};
use rand::{StdRng, Rng};
use std::collections::BTreeMap;
use std::env::args;
use std::fs;
use std::io::{self, BufRead};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};

macro_rules! status_line {($($args: tt)+) => {if *ISATTY {status_line (file!(), line!(), fomat! ($($args)+))}}}

fn main() {
  let dsns: Vec<String> = (io::BufReader::new (fs::File::open ("../pg_async.dsns") .expect ("!pg_async.dsns"))
    .lines().collect() :Result<Vec<String>, _>) .expect ("!pg_async.dsns");
  let cluster = Cluster::new() .expect ("!Cluster");
  for dsn in dsns {cluster.connect (dsn.clone(), 1) .expect ("!connect")}

  println! ("This program just keeps talking with the database servers forever.");

  let pin: Option<u8> = args().find (|a| a.starts_with ("--pin=")) .map (|a| (&a[6..]).parse().expect ("!parse"));
  if let Some (pin) = pin {println! ("Operations are pinned to connection {}.", pin);}

  let mut rng = StdRng::new().expect ("!rng");
  let mut slash = '/';
  let finished = Arc::new (AtomicU64::new (0));
  let errors = Arc::new (AtomicU64::new (0));
  let pool = CpuPool::new (1);
  let in_flight = Arc::new (Mutex::new (BTreeMap::new()));
  loop {
    for _ in 0..99 {  // Generate a decent load spike to ensure that all the connections are busy and pipelining.
      let rid = rng.next_u64();

      let f = match pin {
        None => cluster.execute (fomat! ("SELECT " (rid) " AS rid")) .expect ("!execute"),
        Some (pin) => {
          cluster.execute (PgOperation {
            scheduling: PgSchedulingMode::PinToConnection (pin),
            statements: 1,
            query_pieces: vec! [P (fomat! ("SELECT " (rid) " AS rid"))]
          }) .expect ("!execute")}};

      let f: Box<Future<Item=(), Error=()> + Send> = {
        let finished = finished.clone();
        let errors = errors.clone();
        let in_flight = in_flight.clone();
        Box::new (f.then (move |r| {
          finished.fetch_add (1, Ordering::Relaxed);
          if r.is_err() {errors.fetch_add (1, Ordering::Relaxed);}
          in_flight.lock().unwrap().remove (&rid);
          futures::future::ok::<(), ()> (())}))};
      in_flight.lock().unwrap().insert (rid, pool.spawn (f));}

    status_line! ((slash)
      " Finished: " (finished.load (Ordering::Relaxed)) "."
      " Errors: " (errors.load (Ordering::Relaxed)) "."
      " In flight: " (in_flight.lock().unwrap().len()) '.');
    slash = match slash {'/' => '-', '-' => '\\', '\\' => '|', '|' | _ => '/'};

    std::thread::sleep (std::time::Duration::from_millis (100))}}
