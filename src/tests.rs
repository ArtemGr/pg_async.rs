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
    results.push ((1, cluster.execute ("SELECT 1")));
    results.push ((2, cluster.execute ("SELECT 2")));
    results.push ((3, cluster.execute ("SELECT 3")));}

  for (expect, pr) in results {
    let pr: PgResult = pr.wait().expect ("!pr") .remove (0);
    assert_eq! (expect, pr.row (0) .col_str (0) .unwrap().parse().unwrap());}}

fn check_send<T: Send>(_: &T) {}
fn check_sync<T: Sync>(_: &T) {}

#[test] fn error() {  // Errors should be returned and should not affect the rest of pipelined statements.
  let cluster = Cluster::new() .expect ("!Cluster");
  check_send (&cluster); check_sync (&cluster);
  for dsn in DSNS.iter() {cluster.connect (dsn.clone(), 1) .expect ("!connect")}
  let f = cluster.execute ("SELECT abrakadabra");
  check_send (&f); check_sync (&f);
  match f.wait() {
    Err (ref err) if err.description().contains ("PGRES_FATAL_ERROR") => {check_send (err); check_sync (err)},  // Expected error.
    x => panic! ("Unexpected result (no error?): {:?}", x)}

  // Check how the presence of errors affects the pipeline.
  let mut ops = Vec::new();
  for i in 0..100 {ops.push (cluster.execute (
    if i % 10 != 0 {format! ("SELECT {}", i)} else {"SELECT abrakadabra".into()}))}
  for (op, i) in ops.into_iter().zip (0..) {
    let rc = op.wait();
    if i % 10 != 0 {
      let pr: PgResult = rc.expect ("!op") .remove (0);
      assert_eq! (i, pr.row (0) .col_str (0) .unwrap().parse().unwrap() :u32);
    } else {
      assert! (rc.is_err());}}}  // Expected error.

#[test] fn dml_durability() {  // Data modifications should be preserved in the face of erroneous statements in the pipeline.
  let cluster = Cluster::new() .expect ("!Cluster");
  for dsn in DSNS.iter() {cluster.connect (dsn.clone(), 1) .expect ("!connect")}

  let _ = cluster.execute ("DROP TABLE pg_async_durability_test") .wait();
  std::thread::sleep (std::time::Duration::from_secs (2));  // BDR needs time to synchronize the DROP TABLE.

  let _ = cluster.execute ("CREATE TABLE pg_async_durability_test (t TEXT NOT NULL PRIMARY KEY)") .wait();
  std::thread::sleep (std::time::Duration::from_secs (2));  // BDR needs time to synchronize the CREATE TABLE.

  let mut ops = Vec::new();
  for i in 0..100 {
    if i % 10 != 0 {
      ops.push (cluster.execute (format! ("INSERT INTO pg_async_durability_test (t) VALUES ('{:02}')", i)))
    } else {
      ops.push (cluster.execute ("SELECT abrakadabra"))}}  // Mix INSERTs with erroneous statements.
  for op in ops {let _ = op.wait();}
  std::thread::sleep (std::time::Duration::from_secs (1));  // Give BDR a bit of time to synchronize the INSERTs.

  let rows = cluster.execute ("SELECT t FROM pg_async_durability_test ORDER BY t") .wait().unwrap().remove (0);
  let mut rows = rows.iter();
  for i in 0..100 {
    if i % 10 != 0 {
      let row = rows.next().unwrap();
      assert_eq! (format! ("{:02}", i), row.col_str (0) .unwrap());}}

  let _ = cluster.execute ("DROP TABLE pg_async_durability_test") .wait();}

#[test] fn transactions() {
  let sql = "\
    CREATE TEMPORARY TABLE pg_async_transactions_test (i INTEGER) ON COMMIT DROP; \
    INSERT INTO pg_async_transactions_test (i) VALUES (1); \
    SAVEPOINT tx1; \
    INSERT INTO pg_async_transactions_test (i) VALUES (2); \
    ROLLBACK TO SAVEPOINT tx1; \
    INSERT INTO pg_async_transactions_test (i) VALUES (3); \
    SELECT SUM (i) FROM pg_async_transactions_test";

  let cluster = Cluster::new() .expect ("!Cluster");
  for dsn in DSNS.iter() {cluster.connect (dsn.clone(), 1) .expect ("!connect")}
  let mut ops = Vec::new();
  for _ in 0..9 {ops.push (cluster.execute ((7, sql)))}
  for op in ops {
    let rows = op.wait().unwrap().remove (6);  // SELECT is the 6th statement.
    assert_eq! (rows.row (0) .col (0), b"4");}}

#[test] fn u8_char_to_json() {
  let cluster = Cluster::new() .expect ("!Cluster");
  for dsn in DSNS.iter() {cluster.connect (dsn.clone(), 1) .expect ("!connect")}
  #[derive(Deserialize)] struct Row {zero: u8, one: u8, a: u8, bom: u8}
  let pr = cluster.execute (
    "SELECT 0::\"char\" AS zero, 1::\"char\" AS one, 'a'::\"char\" AS a, E'\\xEF\\xBB\\xBF'::\"char\" AS bom") .wait().unwrap();
  let row: Row = json::from_value (pr[0].row (0) .to_json().unwrap()) .unwrap();
  assert_eq! (row.zero, 0);
  assert_eq! (row.one, 1);
  assert_eq! (row.a, b'a');
  assert_eq! (row.bom, 0xEF);}
