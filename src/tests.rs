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

  let mut results = Vec::new();

  for _ in 0..10 {
    results.push ((1, cluster.execute ("SELECT 1".into()) .expect ("!select 1")));
    results.push ((2, cluster.execute ("SELECT 2".into()) .expect ("!select 2")));
    results.push ((3, cluster.execute ("SELECT 3".into()) .expect ("!select 3")));}

  for (expect, pr) in results {
    let pr: PgResult = pr.wait().expect ("!pr");
    let value = unsafe {CStr::from_ptr (pq::PQgetvalue (pr.0, 0, 0))} .to_str() .expect ("!value");
    assert_eq! (expect, value.parse().unwrap());}}
