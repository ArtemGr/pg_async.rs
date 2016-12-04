use std::fs;
use std::io::{self, BufRead};
use super::*;

lazy_static! {
  static ref DSNS: Vec<String> = (io::BufReader::new (fs::File::open ("../pg_async.dsns") .expect ("!pg_async.dsns"))
    .lines().collect() :Result<Vec<String>, _>) .expect ("!pg_async.dsns");}

#[test] fn select1() {
  let mut cluster = Cluster::new() .expect ("!Cluster");
  for dsn in DSNS.iter() {
    cluster.connect (dsn.clone(), 1) .expect ("!connect")}

  for _ in 0..10 {
    cluster.execute ("SELECT 1".into()) .expect ("!select 1");
    cluster.execute ("SELECT 2".into()) .expect ("!select 2");
    cluster.execute ("SELECT 3".into()) .expect ("!select 3");}

  println! ("Final sleep.");
  std::thread::sleep (std::time::Duration::from_secs (1));
  println! ("Bye.");}
