// cf. https://gist.github.com/ArtemGr/179fb669fa1b065fd2c11a6fd474da8a
// libpq is "too simple-minded" to pipeline on it's own: https://www.postgresql.org/message-id/26269.1356282651@sss.pgh.pa.us
// NB: There is a work in progress on a better pipelining support in libpq: https://commitfest.postgresql.org/10/634/
//     http://2ndquadrant.github.io/postgres/libpq-batch-mode.html

#![feature(type_ascription, integer_atomics)]

extern crate futures;
#[macro_use] extern crate gstuff;
extern crate itertools;
#[macro_use] extern crate lazy_static;
extern crate libc;
extern crate nix;
extern crate serde_json;

use futures::{Future, Poll, Async};
use futures::task::{park, Task};
use libc::{c_char, c_int, c_void};
use nix::poll::{self, EventFlags, PollFd};
use nix::fcntl::{O_NONBLOCK, O_CLOEXEC};
//use serde_json::{Value as Json};
use std::collections::VecDeque;
use std::convert::From;
use std::error::Error;
use std::ffi::{CString, CStr};
use std::fmt;
use std::io;
use std::ptr::null_mut;
use std::slice::from_raw_parts;
use std::str::Utf8Error;
use std::sync::{Arc, Mutex, PoisonError};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver, TryRecvError};
use std::thread::JoinHandle;

#[cfg(test)] mod tests;

/// A part of an SQL query text. The query is constructed by adding `Plain` pieces "as is" and escaping the `Literal` pieces.
///
/// For example, `vec! [PgQueryPiece::Plain ("SELECT ".into()), PgQueryPiece::Literal ("foo".into)]` becomes "SELECT 'foo'".
#[derive(Debug)]
pub enum PgQueryPiece {
  /// Static strings are included in the query "as is".
  Static (&'static str),
  /// Plain strings are included in the query "as is".
  Plain (String),
  /// Literals are escaped with `PQescapeLiteral` (which also places them into the single quotes).
  Literal (String)}

/// Converts the `fn execute` input into a vector of query pieces.
pub trait IntoQueryPieces {
  /// Returns the number of SQL statements (the library must know how many results to expect) and the list of pieces to escape and join.
  fn into_query_pieces (self) -> (u32, Vec<PgQueryPiece>);}

impl IntoQueryPieces for String {
  fn into_query_pieces (self) -> (u32, Vec<PgQueryPiece>) {
    (1, vec! [PgQueryPiece::Plain (self)])}}

impl IntoQueryPieces for &'static str {
  fn into_query_pieces (self) -> (u32, Vec<PgQueryPiece>) {
    (1, vec! [PgQueryPiece::Static (self)])}}

impl<'a> IntoQueryPieces for Vec<PgQueryPiece> {
  fn into_query_pieces (self) -> (u32, Vec<PgQueryPiece>) {(1, self)}}

impl IntoQueryPieces for (u32, &'static str) {
  fn into_query_pieces (self) -> (u32, Vec<PgQueryPiece>) {
    (self.0, vec! [PgQueryPiece::Static (self.1)])}}

impl IntoQueryPieces for (u32, String) {
  fn into_query_pieces (self) -> (u32, Vec<PgQueryPiece>) {
    (self.0, vec! [PgQueryPiece::Plain (self.1)])}}

impl IntoQueryPieces for (u32, Vec<PgQueryPiece>) {
  fn into_query_pieces (self) -> (u32, Vec<PgQueryPiece>) {self}}

pub struct PgRow<'a> (&'a PgResult, u32);

impl<'a> PgRow<'a> {
  /// True if the value is NULL.
  pub fn is_null (&self, column: u32) -> bool {
    if column > self.0.columns {panic! ("Column index {} is out of range (0..{})", column, self.0.columns)}
    1 == unsafe {pq::PQgetisnull ((self.0).res, self.1 as c_int, column as c_int)}}
  /// PostgreSQL internal OID number of the column type.
  pub fn ftype (&self, column: u32) -> pq::Oid {
    if column > self.0.columns {panic! ("Column index {} is out of range (0..{})", column, self.0.columns)}
    unsafe {pq::PQftype ((self.0).res, column as c_int)}}
  /// Returns an empty array if the value is NULL.
  pub fn col (&self, column: u32) -> &'a [u8] {
    if column > self.0.columns {panic! ("Column index {} is out of range (0..{})", column, self.0.columns)}
    let len = unsafe {pq::PQgetlength ((self.0).res, self.1 as c_int, column as c_int)};
    let val = unsafe {pq::PQgetvalue ((self.0).res, self.1 as c_int, column as c_int)};
    unsafe {from_raw_parts (val as *const u8, len as usize)}}
  /// Returns an empty string if the value is NULL.
  pub fn col_str (&self, column: u32) -> Result<&'a str, std::str::Utf8Error> {
    if column > self.0.columns {panic! ("Column index {} is out of range (0..{})", column, self.0.columns)}
    unsafe {CStr::from_ptr (pq::PQgetvalue ((self.0).res, self.1 as c_int, column as c_int))} .to_str()}}

pub struct PgResult {
  pub res: *mut pq::PGresult,
  pub rows: u32,
  pub columns: u32}

// cf. https://www.postgresql.org/docs/9.4/static/libpq-threading.html
unsafe impl Sync for PgResult {}
unsafe impl Send for PgResult {}

impl PgResult {
  /// True if there are no rows in the result.
  pub fn is_empty (&self) -> bool {self.rows == 0}
  /// Number of rows.
  pub fn len (&self) -> u32 {self.rows}
  /// PostgreSQL internal OID number of the column type.
  pub fn ftype (&self, column: u32) -> pq::Oid {
    if column > self.columns {panic! ("Column index {} is out of range (0..{})", column, self.columns)}
    unsafe {pq::PQftype (self.res, column as c_int)}}
  pub fn row (&self, row: u32) -> PgRow {
    if row >= self.rows {panic! ("Row index {} is out of range (0..{})", row, self.rows)}
    PgRow (self, row)}
  /// Iterator over result rows.
  pub fn iter<'a> (&'a self) -> PgResultIt<'a> {
    PgResultIt {pr: self, row: 0}}}

impl Drop for PgResult {
  fn drop (&mut self) {
    assert! (self.res != null_mut());
    unsafe {pq::PQclear (self.res)}}}

impl fmt::Debug for PgResult {
  fn fmt (&self, ft: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    write! (ft, "PgResult")}}

/// An `Iterator` over `PgResult` rows.
pub struct PgResultIt<'a> {pr: &'a PgResult, row: u32}
impl<'a> Iterator for PgResultIt<'a> {
  type Item = PgRow<'a>;
  fn next (&mut self) -> Option<PgRow<'a>> {
    if self.row < self.pr.rows {
      let row = PgRow (self.pr, self.row);
      self.row += 1;
      Some (row)
    } else {None}}}

struct PgFutureSync {
  results: Vec<PgResult>,
  task: Option<Task>}

struct PgFutureImpl {
  id: u64,
  /// The number of separate top-level SQL statements withing the `sql`. E.g. the number of results PostgreSQL will return.
  statements: u32,
  sql: Vec<PgQueryPiece>,
  sync: Mutex<PgFutureSync>}

/// The part of `PgFutureErr` that's used when the SQL fails.
pub struct PgSqlErr {
  /// Pointer to the failed future, which we keep in order to print the SQL in `Debug`.
  imp: Arc<PgFutureImpl>,
  /// Which statement has failed.
  ///
  /// In a single-statement ops this is usually `0` for the statement itself or `1` for the COMMIT.
  pub num: u32,
  /// Returned by `PQresStatus`, "string constant describing the status code".
  pub status: &'static str,
  pub message: String  }

/// Returned when the future fails. Might print the SQL in `Debug`.
pub enum PgFutureErr {
  PoisonError,
  Utf8Error (Utf8Error),
  Sql (PgSqlErr)}

impl fmt::Debug for PgFutureErr {
  fn fmt (&self, ft: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    match self {
      &PgFutureErr::PoisonError => ft.write_str ("PgFutureErr::PoisonError"),
      &PgFutureErr::Utf8Error (_) => ft.write_str ("PgFutureErr::Utf8Error"),
      &PgFutureErr::Sql (ref se) => write! (ft, "PgFutureErr ({:?}, {})", se.imp.sql, se.message)}}}

impl fmt::Display for PgFutureErr {
  fn fmt (&self, ft: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    match self {
      &PgFutureErr::PoisonError => ft.write_str ("PgFutureErr::PoisonError"),
      &PgFutureErr::Utf8Error (_) => ft.write_str ("PgFutureErr::Utf8Error"),
      &PgFutureErr::Sql (ref se) => ft.write_str (&se.message)}}}

impl Error for PgFutureErr {
  fn description (&self) -> &str {
    match self {
      &PgFutureErr::PoisonError => "PgFutureErr::PoisonError",
      &PgFutureErr::Utf8Error (_) => "PgFutureErr::Utf8Error",
      &PgFutureErr::Sql (ref se) => &se.message[..]}}}

impl<T> From<PoisonError<T>> for PgFutureErr {
  fn from (_err: PoisonError<T>) -> PgFutureErr {
    PgFutureErr::PoisonError}}

impl From<Utf8Error> for PgFutureErr {
  fn from (err: Utf8Error) -> PgFutureErr {
    PgFutureErr::Utf8Error (err)}}

/// Delayed SQL result.
#[derive(Clone)]
pub struct PgFuture (Arc<PgFutureImpl>);

impl PgFuture {
  fn new (id: u64, statements: u32, sql: Vec<PgQueryPiece>) -> PgFuture {
    PgFuture (Arc::new (PgFutureImpl {
      id: id,
      statements: statements,
      sql: sql,
      sync: Mutex::new (PgFutureSync {
        results: Vec::new(),
        task: None})}))}}

impl fmt::Debug for PgFuture {
  fn fmt (&self, ft: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    write! (ft, "PgFuture ({}, {:?})", self.0.id, self.0.sql)}}

impl Future for PgFuture {
  type Item = Vec<PgResult>;
  type Error = PgFutureErr;
  fn poll (&mut self) -> Poll<Vec<PgResult>, PgFutureErr> {
    let mut sync = self.0.sync.lock()?;

    if sync.results.is_empty() {
      sync.task = Some (park());
      return Ok (Async::NotReady)}

    for (pr, num) in sync.results.iter().zip (0..) {
      if let Some (status) = error_in_result (pr.res) {
        let status = unsafe {CStr::from_ptr (pq::PQresStatus (status))} .to_str()?;
        let err = unsafe {CStr::from_ptr (pq::PQresultErrorMessage (pr.res))} .to_str()?;
        return Err (PgFutureErr::Sql (PgSqlErr {
          imp: self.0.clone(),
          num: num,
          status: status,
          message: format! ("Error in statement {}: {}; {}", num, status, err)}))}}

    let mut res = Vec::with_capacity (sync.results.len());
    res.append (&mut sync.results);

    Ok (Async::Ready (res))}}

#[derive(Debug)]
enum Message {
  Connect (String, u8),
  Execute (PgFuture),
  Drop}

#[derive(Debug)]
struct Connection {
  conn: *mut pq::PGconn,
  in_flight: VecDeque<PgFuture>,
  /// True if there is an internal statement in flight.
  in_flight_init: bool}

impl Connection {
  fn free (&self) -> bool {
    self.in_flight.is_empty() && !self.in_flight_init}}

fn error_in_result (res: *const pq::PGresult) -> Option<pq::ExecStatusType> {
  let status = unsafe {pq::PQresultStatus (res)};
  if status != pq::PGRES_COMMAND_OK && status != pq::PGRES_TUPLES_OK {Some (status)} else {None}}

// Limit the size of the pipeline. This is especially important in the presence of errors,
// because we have to reschedule the majority of the commands after an error.
const PIPELINE_LIM_COMMANDS: usize = 128;
const PIPELINE_LIM_BYTES: usize = 16384;

fn event_loop (rx: Receiver<Message>, read_end: c_int) {
  // NB: Connection must not leak into other threads.
  // "One thread restriction is that no two threads attempt to manipulate the same PGconn object at the same time.
  // In particular, you cannot issue concurrent commands from different threads through the same connection object.
  // (If you need to run concurrent commands, use multiple connections.)"
  use std::fmt::Write;

  let mut pending_connections: Vec<*mut pq::PGconn> = Vec::new();
  let mut good_connections: Vec<Connection> = Vec::new();
  let mut fds = Vec::new();
  let mut pending_futures: VecDeque<PgFuture> = VecDeque::new();
  let mut sql = String::with_capacity (PIPELINE_LIM_BYTES + 1024);
  let mut sql_futures = Vec::with_capacity (PIPELINE_LIM_COMMANDS);
  'event_loop: loop {
    { let mut tmp: [u8; 256] = unsafe {std::mem::uninitialized()}; let _ = nix::unistd::read (read_end, &mut tmp); }
    loop {match rx.try_recv() {
      Err (TryRecvError::Disconnected) => break 'event_loop,  // Cluster was dropped.
      Err (TryRecvError::Empty) => break,
      Ok (message) => match message {
        Message::Connect (dsn, mul) => {
          // TODO: Keep a copy of `dsn` and `mul` in case we'll need to recreate a failed connection.
          let dsn = CString::new (dsn) .expect ("!dsn");
          for _ in 0 .. mul {
            let conn = unsafe {pq::PQconnectStart (dsn.as_ptr())};
            if conn == null_mut() {panic! ("!PQconnectStart")}
            pending_connections.push (conn);}},
        Message::Execute (pg_future) => pending_futures.push_back (pg_future),
        Message::Drop => break 'event_loop}}}  // Cluster is dropping.

    fds.clear();

    for idx in (0 .. pending_connections.len()) .rev() {
      let conn: *mut pq::PGconn = pending_connections[idx];
      let status = unsafe {pq::PQstatus (conn)};
      if status == pq::CONNECTION_BAD {
        panic! ("CONNECTION_BAD: {}", error_message (conn));
        //pending_connections.remove (idx);
      } else if status == pq::CONNECTION_OK {
        let rc = unsafe {pq::PQsetnonblocking (conn, 1)};
        if rc != 0 {panic! ("!PQsetnonblocking: {}", error_message (conn))}

        // Pipelining usually works by executing all queries in a single transaction, but we might want to avoid that
        // because we don't want a single failing query to invalidate (roll back) all DMLs that happen to share the same pipeline.
        // This means we might be wrapping each query in a transaction
        // and need some other means to amortize the const of many transactions being present in the pipeline.
        //
        // BDR defaults to asynchronous commits anyway, cf. http://bdr-project.org/docs/stable/bdr-configuration-variables.html#GUC-BDR-SYNCHRONOUS-COMMIT
        let rc = unsafe {pq::PQsendQuery (conn, "SET synchronous_commit = off\0".as_ptr() as *const c_char)};
        if rc == 0 {panic! ("!PQsendQuery: {}", error_message (conn))}

        good_connections.push (Connection {conn: conn, in_flight: VecDeque::new(), in_flight_init: true});
        pending_connections.remove (idx);
      } else {
        let sock = unsafe {pq::PQsocket (conn)};
        match unsafe {pq::PQconnectPoll (conn)} {
          pq::PGRES_POLLING_READING => fds.push (PollFd::new (sock, poll::POLLIN, EventFlags::empty())),
          pq::PGRES_POLLING_WRITING => fds.push (PollFd::new (sock, poll::POLLOUT, EventFlags::empty())),
          _ => ()};}}

    // Try to find a connection that isn't currently busy.
    // (As of now, only one `PQsendQuery` can be in flight).
    for conn in good_connections.iter_mut().filter (|conn| conn.free()) {
      if pending_futures.is_empty() {break}
      // Join all the queries into a single batch.
      // Similar to how libpqxx pipelining works, cf.
      // https://github.com/jtv/libpqxx/blob/master/include/pqxx/pipeline.hxx
      // https://github.com/jtv/libpqxx/blob/master/src/pipeline.cxx
      sql.clear();
      sql_futures.clear();
      let mut first = true;
      loop {
        if sql.len() >= PIPELINE_LIM_BYTES {break}
        if sql_futures.len() + 1 >= PIPELINE_LIM_COMMANDS {break}
        let pending = match pending_futures.pop_front() {Some (f) => f, None => break};
        if first {first = false} else {sql.push_str ("; ")}
        write! (&mut sql, "BEGIN; SELECT {} AS future_id; ", pending.0.id) .expect ("!write");
        for piece in pending.0.sql.iter() {
          match piece {
            &PgQueryPiece::Static (ref ss) => sql.push_str (ss),
            &PgQueryPiece::Plain (ref plain) => sql.push_str (&plain),
            &PgQueryPiece::Literal (ref literal) => {
              let esc = unsafe {pq::PQescapeLiteral (conn.conn, literal.as_ptr() as *const c_char, literal.len())};
              if esc == null_mut() {panic! ("!PQescapeLiteral: {}", error_message (conn.conn))}
              sql.push_str (unsafe {CStr::from_ptr (esc)} .to_str().expect ("!esc"));
              unsafe {pq::PQfreemem (esc as *mut c_void)};}}}
        // Wrap every command in a separate transaction in order not to loose the DML changes when there is an erroneous statement in the pipeline.
        sql.push_str ("; COMMIT");
        sql_futures.push (pending)}

      // NB: We need this call to flip a flag in libpq, particularly when the loop that process the results finishes early.
      let res = unsafe {pq::PQgetResult (conn.conn)};
      if res != null_mut() {panic! ("Stray result detected before PQsendQuery! {:?}", res);}

      let sql = CString::new (&sql[..]) .expect ("!sql");
      let rc = unsafe {pq::PQsendQuery (conn.conn, sql.as_ptr())};
      if rc == 0 {panic! ("!PQsendQuery: {}", error_message (conn.conn))}
      conn.in_flight.extend (sql_futures.drain (..));}

    for conn in good_connections.iter_mut() {
      if !conn.free() {
        let sock = unsafe {pq::PQsocket (conn.conn)};

        let rc = unsafe {pq::PQconsumeInput (conn.conn)};
        if rc != 1 {panic! ("!PQconsumeInput: {}", error_message (conn.conn))}

        let rc = unsafe {pq::PQflush (conn.conn)};
        if rc == 1 {fds.push (PollFd::new (sock, poll::POLLOUT, EventFlags::empty()))}

        loop {
          if unsafe {pq::PQisBusy (conn.conn)} == 1 {break}
          let mut error = false;
          let mut after_future = false;
          if conn.in_flight_init {
            conn.in_flight_init = false;
            let res = unsafe {pq::PQgetResult (conn.conn)};
            if res == null_mut() {panic! ("Got no result for in_flight_init statement")}
            error = error_in_result (res) .is_some()
          } else {
            let pg_future = match conn.in_flight.pop_front() {Some (f) => f, None => break};
            let mut sync = pg_future.0.sync.lock().expect ("!lock");
            after_future = true;

            // Every statement is wrapped in a "BEGIN; SELECT $id; $statement; COMMIT" transaction and produces not one but three results.

            { let begin_res = unsafe {pq::PQgetResult (conn.conn)};
              if begin_res == null_mut() {panic! ("Got no BEGIN result for {:?}", pg_future)}
              if let Some (error_status) = error_in_result (begin_res) {
                // BEGIN fails when there is a syntax error in the pipeline SQL.
                // The error might be in a statement that exists later in the pipeline and is not related to `pg_future`,
                // but to properly match the error to the right `pg_future` we'd have to parse the error message for the exact location of the error
                // (e.g. 62 in 'syntax error at or near "group" at character 62') and trace it back to `pg_future` by knowing which part
                // of the pipeline SQL belongs to it.
                // Hopefully the libpq pipelining patch from 2ndquadrant will make it simpler.

                // We can do a shortcut and just fail the query that happens to be at the head of the pipeline, like this:
                //sync.results.push (PgResult {  // NB: `PgResult` takes responsibility to `PQclear` the `statement_res`.
                //  res: begin_res, rows: 0, columns: 0});
                //error = true;

                // But for now it might be better if we just panic. SQL is code and syntax errors aren't usually expected.
                let status = unsafe {CStr::from_ptr (pq::PQresStatus (error_status))} .to_str() .expect ("!to_str");
                let err = unsafe {CStr::from_ptr (pq::PQresultErrorMessage (begin_res))} .to_str() .expect ("!to_str");
                panic! ("BEGIN failed. Probably a syntax error in one of the pipelined SQL statements. {},\n{}", status, err);}
              unsafe {pq::PQclear (begin_res)} }

            if !error { // Check that we're getting results for the right future.
              let id_res = unsafe {pq::PQgetResult (conn.conn)};
              if id_res == null_mut() {panic! ("Got no ID result for {:?}", pg_future)}
              if error_in_result (id_res) .is_some() {panic! ("Error in ID")}
              assert_eq! (unsafe {pq::PQntuples (id_res)}, 1);
              assert_eq! (unsafe {pq::PQnfields (id_res)}, 1);
              let id = unsafe {CStr::from_ptr (pq::PQgetvalue (id_res, 0, 0))} .to_str() .expect ("!to_str");
              let id: u64 = id.parse().expect ("!id");
              assert_eq! (id, pg_future.0.id);  // The check.
              unsafe {pq::PQclear (id_res)} }

            let expect_results = pg_future.0.statements as usize + 1;
            sync.results.reserve_exact (expect_results);
            for num in 0..expect_results {
              if error {break}
              let statement_res = unsafe {pq::PQgetResult (conn.conn)};
              if statement_res == null_mut() {panic! ("Got no statement {} result for {:?}", num, pg_future)}
              error = error_in_result (statement_res) .is_some();
              sync.results.push (PgResult {  // NB: `PgResult` takes responsibility to `PQclear` the `statement_res`.
                res: statement_res,
                rows: unsafe {pq::PQntuples (statement_res)} as u32,
                columns: unsafe {pq::PQnfields (statement_res)} as u32});}

            if let Some (ref task) = sync.task {task.unpark()}}

          if error {
            if after_future {
              // Need to call `PQgetResult` one last time in order to flip a flag in libpq. Otherwise `PQsendQuery` won't work.
              let res = unsafe {pq::PQgetResult (conn.conn)};
              if res != null_mut() {panic! ("Unexpected result after an error")}

              // We're using BEGIN-COMMIT, so we should ROLLBACK after a failure.
              // Otherwise we'll see "current transaction is aborted, commands ignored until end of transaction block".
              let rc = unsafe {pq::PQsendQuery (conn.conn, "ROLLBACK\0".as_ptr() as *const c_char)};
              if rc == 0 {panic! ("!PQsendQuery: {}", error_message (conn.conn))}
              conn.in_flight_init = true}

            // If a statement fails then the rest of the pipeline fails. Reschedule the remaining futures.
            pending_futures.extend (conn.in_flight.drain (..))}}

        fds.push (PollFd::new (sock, poll::POLLIN, EventFlags::empty()))}}

    fds.push (PollFd::new (read_end, poll::POLLIN, EventFlags::empty()));
    let rc = poll::poll (&mut fds, 100) .expect ("!poll");
    if rc == -1 {panic! ("!poll: {}", io::Error::last_os_error())}}

  for conn in good_connections {unsafe {pq::PQfinish (conn.conn)}}
  for conn in pending_connections {unsafe {pq::PQfinish (conn)}}}

/// A cluster of several replicated PostgreSQL nodes.
pub struct Cluster {
  /// Runs the event loop.
  thread: Option<JoinHandle<()>>,
  /// Channel to the `thread`.
  tx: Mutex<Sender<Message>>,
  /// pipe2 file descriptor used to wake the poll thread.
  write_end: c_int,
  /// Used to generate the unique identifiers for the asynchronous SQL commands.
  command_num: AtomicU64}

impl Cluster {
  /// Start the thread.
  pub fn new() -> Result<Cluster, String> {
    let (tx, rx) = channel();
    let (read_end, write_end) = try_s! (nix::unistd::pipe2 (O_NONBLOCK | O_CLOEXEC));
    let thread = try_s! (std::thread::Builder::new().name ("pg_async".into()) .spawn (move || event_loop (rx, read_end)));
    Ok (Cluster {
      thread: Some (thread),
      tx: Mutex::new (tx),
      write_end: write_end,
      command_num: AtomicU64::new (0)})}

  /// Setup a database connection to a [replicated] node.
  ///
  /// Replicated clusters have several nodes usually. This method should be used for each node.
  ///
  /// Nodes can be added on the go. E.g. add a first node, fire some queries, add a second node.
  ///
  /// * `dsn` - A keyword=value [connection string](https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-CONNSTRING).
  /// * `mul` - Pipelining support in `libpq` is currently limited to one batch of queries per connection,
  ///           but parallelism can be increased by adding the same connection several times.
  pub fn connect (&self, dsn: String, mul: u8) -> Result<(), String> {
    try_s! (try_s! (self.tx.lock()) .send (Message::Connect (dsn, mul)));
    try_s! (nix::unistd::write (self.write_end, &[1]));
    Ok(())}

  /// Schedule an SQL command to be executed on one of the nodes.
  ///
  /// SQL will be run inside a transaction.
  ///
  /// When using multiple statements, the library user must specify the exact number of top-level statements that the PostgreSQL server is going to see.
  /// For example:
  ///
  /// ```
  ///   cluster.execute ((2, "SELECT 1; SELECT 2"));  // Running two statements as a single op.
  ///   cluster.execute ((3, "DELETE FROM foo; INSERT INTO foo VALUES (1); INSERT INTO foo VALUES (2)"));  // Running three statements.
  /// ```
  ///
  /// To avoid SQL injection one might use the escapes provided by `PgQueryPiece`:
  ///
  /// ```
  ///   use pg_async::PgQueryPiece::{Static as S, Literal as L};
  ///   cluster.execute (vec! [S ("SELECT * FROM foo WHERE bar = "), L (bar)]);
  /// ```
  pub fn execute<I: IntoQueryPieces> (&self, sql: I) -> Result<PgFuture, String> {
    self.command_num.compare_and_swap (u64::max_value(), 0, Ordering::Relaxed);  // Recycle the set of identifiers.
    let id = self.command_num.fetch_add (1, Ordering::Relaxed) + 1;
    let (statements, pieces) = sql.into_query_pieces();
    let pg_future = PgFuture::new (id, statements, pieces);

    try_s! (try_s! (self.tx.lock()) .send (Message::Execute (pg_future.clone())));
    try_s! (nix::unistd::write (self.write_end, &[2]));
    Ok (pg_future)}}

impl Drop for Cluster {
  fn drop (&mut self) {
    if let Ok (tx) = self.tx.lock() {
      let _ = tx.send (Message::Drop);}
    let _ = nix::unistd::write (self.write_end, &[3]);
    let mut thread = None;
    std::mem::swap (&mut thread, &mut self.thread);
    if let Some (thread) = thread {let _ = thread.join();}}}

fn error_message (conn: *const pq::PGconn) -> String {
  let err = unsafe {pq::PQerrorMessage (conn)};
  let err = unsafe {CStr::from_ptr (err)};
  err.to_string_lossy().into()}

/// FFI bindings to `libpq`.
pub mod pq {  // cf. "bindgen /usr/include/postgresql/libpq-fe.h".
  use libc::{c_char, c_int, c_uint, c_void, size_t};

  pub enum PGconn {}

  pub type ConnStatusType = c_uint;
  pub const CONNECTION_OK: c_uint = 0;
  pub const CONNECTION_BAD: c_uint = 1;
  pub const CONNECTION_STARTED: c_uint = 2;
  pub const CONNECTION_MADE: c_uint = 3;
  pub const CONNECTION_AWAITING_RESPONSE: c_uint = 4;
  pub const CONNECTION_AUTH_OK: c_uint = 5;
  pub const CONNECTION_SETENV: c_uint = 6;
  pub const CONNECTION_SSL_STARTUP: c_uint = 7;
  pub const CONNECTION_NEEDED: c_uint = 8;

  pub type PostgresPollingStatusType = c_uint;
  pub const PGRES_POLLING_FAILED: c_uint = 0;
  pub const PGRES_POLLING_READING: c_uint = 1;
  pub const PGRES_POLLING_WRITING: c_uint = 2;
  pub const PGRES_POLLING_OK: c_uint = 3;
  pub const PGRES_POLLING_ACTIVE: c_uint = 4;

  pub enum PGresult {}

  pub type ExecStatusType = c_uint;
  pub const PGRES_EMPTY_QUERY: c_uint = 0;
  pub const PGRES_COMMAND_OK: c_uint = 1;
  pub const PGRES_TUPLES_OK: c_uint = 2;
  pub const PGRES_COPY_OUT: c_uint = 3;
  pub const PGRES_COPY_IN: c_uint = 4;
  pub const PGRES_BAD_RESPONSE: c_uint = 5;
  pub const PGRES_NONFATAL_ERROR: c_uint = 6;
  pub const PGRES_FATAL_ERROR: c_uint = 7;
  pub const PGRES_COPY_BOTH: c_uint = 8;
  pub const PGRES_SINGLE_TUPLE: c_uint = 9;

  pub type Oid = c_uint;

  #[link(name = "pq")] extern {
    /// https://www.postgresql.org/docs/9.4/static/libpq-threading.html
    pub fn PQisthreadsafe() -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-PQCONNECTDB
    pub fn PQconnectdb (conninfo: *const c_char) -> *mut PGconn;
    /// https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-PQCONNECTSTARTPARAMS
    pub fn PQconnectStart (conninfo: *const c_char) -> *mut PGconn;
    /// https://www.postgresql.org/docs/9.4/static/libpq-status.html#LIBPQ-PQSTATUS
    pub fn PQstatus (conn: *const PGconn) -> ConnStatusType;
    /// https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-PQCONNECTSTARTPARAMS
    pub fn PQconnectPoll (conn: *mut PGconn) -> PostgresPollingStatusType;
    /// https://www.postgresql.org/docs/9.4/static/libpq-status.html#LIBPQ-PQSOCKET
    pub fn PQsocket (conn: *const PGconn) -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-async.html#LIBPQ-PQSETNONBLOCKING
    pub fn PQsetnonblocking (conn: *mut PGconn, arg: c_int) -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-PQFINISH
    pub fn PQfinish (conn: *mut PGconn);
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-EXEC-ESCAPE-STRING
    pub fn PQescapeLiteral (conn: *mut PGconn, str: *const c_char, len: size_t) -> *mut c_char;
    /// https://www.postgresql.org/docs/9.4/static/libpq-misc.html#LIBPQ-PQFREEMEM
    pub fn PQfreemem (ptr: *mut c_void);
    /// https://www.postgresql.org/docs/9.4/static/libpq-async.html#LIBPQ-PQSENDQUERY
    pub fn PQsendQuery (conn: *mut PGconn, command: *const c_char) -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-async.html#LIBPQ-PQFLUSH
    pub fn PQflush (conn: *mut PGconn) -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-async.html#LIBPQ-PQCONSUMEINPUT
    pub fn PQconsumeInput (conn: *mut PGconn) -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-async.html#LIBPQ-PQISBUSY
    pub fn PQisBusy (conn: *mut PGconn) -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-async.html#LIBPQ-PQGETRESULT
    pub fn PQgetResult (conn: *mut PGconn) -> *mut PGresult;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQRESULTSTATUS
    pub fn PQresultStatus (res: *const PGresult) -> ExecStatusType;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQRESSTATUS
    pub fn PQresStatus (status: ExecStatusType) -> *mut c_char;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQRESULTERRORMESSAGE
    pub fn PQresultErrorMessage (res: *const PGresult) -> *mut c_char;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQNTUPLES
    pub fn PQntuples (res: *const PGresult) -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQNFIELDS
    pub fn PQnfields (res: *const PGresult) -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQFNAME
    pub fn PQfname (res: *const PGresult, field_num: c_int) -> *mut c_char;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQFTYPE
    pub fn PQftype (res: *const PGresult, field_num: c_int) -> Oid;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQGETISNULL
    pub fn PQgetisnull (res: *const PGresult, tup_num: c_int, field_num: c_int) -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQGETVALUE
    pub fn PQgetvalue (res: *const PGresult, tup_num: c_int, field_num: c_int) -> *mut c_char;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQGETLENGTH
    pub fn PQgetlength (res: *const PGresult, tup_num: c_int, field_num: c_int) -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQCMDSTATUS
    pub fn PQcmdStatus (res: *mut PGresult) -> *mut c_char;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQCMDTUPLES
    pub fn PQcmdTuples (res: *mut PGresult) -> *mut c_char;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQOIDVALUE
    pub fn PQoidValue (res: *const PGresult) -> Oid;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQCLEAR
    pub fn PQclear (result: *mut PGresult);
    /// https://www.postgresql.org/docs/9.4/static/libpq-status.html#LIBPQ-PQERRORMESSAGE
    pub fn PQerrorMessage (conn: *const PGconn) -> *const c_char;
} }
