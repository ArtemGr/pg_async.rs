use std::fs;
use std::io::{self, BufRead};
use super::*;

lazy_static! {
  static ref DSNS: Vec<String> = (io::BufReader::new (fs::File::open ("../pg_async.dsns") .expect ("!pg_async.dsns"))
    .lines().collect() :Result<Vec<String>, _>) .expect ("!pg_async.dsns");}

#[test] fn select1() {
  let cluster = Cluster::new() .expect ("!Cluster");
  for dsn in DSNS.iter() {cluster.connect (dsn.clone(), 1) .expect ("!connect")}

  let mut results = Vec::new();

  for _ in 0..10 {
    results.push ((1, cluster.execute ("SELECT 1") .expect ("!select 1")));
    results.push ((2, cluster.execute ("SELECT 2") .expect ("!select 2")));
    results.push ((3, cluster.execute ("SELECT 3") .expect ("!select 3")));}

  for (expect, pr) in results {
    let pr: PgResult = pr.wait().expect ("!pr");
    assert_eq! (expect, pr.row (0) .col_str (0) .unwrap().parse().unwrap());}}

#[test] fn error() {
  let cluster = Cluster::new() .expect ("!Cluster");
  for dsn in DSNS.iter() {cluster.connect (dsn.clone(), 1) .expect ("!connect")}
  let f = cluster.execute ("SELECT abrakadabra") .expect ("!execute");
  match f.wait() {
    Err (ref err) if err.contains ("PGRES_FATAL_ERROR") => (),  // Expected error.
    x => panic! ("Unexpected result (no error?): {:?}", x)}

  let mut ops = Vec::new();
  for i in 0..100 {ops.push (cluster.execute (
    if i % 10 != 0 {format! ("SELECT {}", i)} else {"SELECT abrakadabra".into()}) .expect ("!execute"))}
  for (op, i) in ops.into_iter().zip (0..) {
    let rc = op.wait();
    if i % 10 != 0 {
      let pr = rc.expect ("!op");
      assert_eq! (i, pr.row (0) .col_str (0) .unwrap().parse().unwrap() :u32);
    } else {
      assert! (rc.is_err());}}}

#[test] fn lack_of_durability() {}
