// cf. https://gist.github.com/ArtemGr/179fb669fa1b065fd2c11a6fd474da8a
// libpq is "too simple-minded" to pipeline on it's own: https://www.postgresql.org/message-id/26269.1356282651@sss.pgh.pa.us
// NB: There is a work in progress on a better pipelining support in libpq: https://commitfest.postgresql.org/10/634/
//     http://2ndquadrant.github.io/postgres/libpq-batch-mode.html

#![feature(type_ascription, integer_atomics)]

#[macro_use] extern crate gstuff;
extern crate itertools;
#[macro_use] extern crate lazy_static;
extern crate libc;
extern crate nix;

use libc::c_int;
use nix::poll::{self, EventFlags, PollFd};
use nix::fcntl::{O_NONBLOCK, O_CLOEXEC};
use std::collections::VecDeque;
use std::ffi::{CString, CStr};
use std::fmt;
use std::ptr::null_mut;
use std::sync::Arc;
use std::sync::atomic::{AtomicPtr, AtomicU64, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver, TryRecvError};
use std::thread::JoinHandle;

#[cfg(test)] mod tests;

struct PgResultImpl {
  id: u64,
  sql: String,
  res: AtomicPtr<pq::PGresult>}
impl Drop for PgResultImpl {
  fn drop (&mut self) {
    let res: *mut pq::PGresult = self.res.swap (null_mut(), Ordering::Relaxed);
    if res != null_mut() {
      unsafe {pq::PQclear (res)}}}}

/// Delayed SQL result.
#[derive(Clone)]
pub struct PgResult (Arc<PgResultImpl>);

impl PgResult {
  fn new (id: u64, sql: String) -> PgResult {
    PgResult (Arc::new (PgResultImpl {
        id: id,
        sql: sql,
        res: AtomicPtr::new (null_mut())}))}}

impl fmt::Debug for PgResult {
  fn fmt (&self, ft: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    write! (ft, "PgResult ({}, {})", self.0.id, self.0.sql)}}

#[derive(Debug)]
enum Message {
  Connect (String, u8),
  Execute (PgResult),
  Drop}

#[derive(Debug)]
struct Connection {
  conn: *mut pq::PGconn,
  in_flight: VecDeque<PgResult>}

fn event_loop (rx: Receiver<Message>, read_end: c_int) {
  // NB: Connection must not leak into other threads.
  // "One thread restriction is that no two threads attempt to manipulate the same PGconn object at the same time.
  // In particular, you cannot issue concurrent commands from different threads through the same connection object.
  // (If you need to run concurrent commands, use multiple connections.)"

  let mut pending_connections: Vec<*mut pq::PGconn> = Vec::new();
  let mut good_connections: Vec<Connection> = Vec::new();
  let mut fds = Vec::new();
  let mut pending_sqls = Vec::new();
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
        Message::Execute (sql) => pending_sqls.push (sql),
        Message::Drop => break 'event_loop}}}  // Cluster is dropping.

    fds.clear();

    for idx in (0 .. pending_connections.len()) .rev() {
      let conn: *mut pq::PGconn = pending_connections[idx];
      let status = unsafe {pq::PQstatus (conn)};
      if status == pq::CONNECTION_BAD {
        println! ("CONNECTION_BAD: {}", error_message (conn));
        pending_connections.remove (idx);
      } else if status == pq::CONNECTION_OK {
        let rc = unsafe {pq::PQsetnonblocking (conn, 1)};
        if rc != 0 {panic! ("!PQsetnonblocking: {}", error_message (conn))}
        good_connections.push (Connection {conn: conn, in_flight: VecDeque::new()});
        pending_connections.remove (idx);
      } else {
        let sock = unsafe {pq::PQsocket (conn)};
        match unsafe {pq::PQconnectPoll (conn)} {
          pq::PGRES_POLLING_READING => fds.push (PollFd::new (sock, poll::POLLIN, EventFlags::empty())),
          pq::PGRES_POLLING_WRITING => fds.push (PollFd::new (sock, poll::POLLOUT, EventFlags::empty())),
          _ => ()};}}

    if !pending_sqls.is_empty() {
      // Try to find a connection that isn't currently busy.
      // (As of now, only one `PQsendQuery` can be in flight).
      if let Some (conn) = good_connections.iter_mut().find (|conn| conn.in_flight.is_empty()) {
        // Join all the queries into a single batch.
        // Similar to how libpqxx pipelining works, cf.
        // https://github.com/jtv/libpqxx/blob/master/include/pqxx/pipeline.hxx
        // https://github.com/jtv/libpqxx/blob/master/src/pipeline.cxx
        let sql = itertools::join (pending_sqls.iter().map (|pr| &pr.0.sql[..]), "; ");
        println! ("SQL batch: {}", sql);
        let sql = CString::new (sql) .expect ("!sql");
        let rc = unsafe {pq::PQsendQuery (conn.conn, sql.as_ptr())};
        if rc == 0 {panic! ("!PQsendQuery: {}", error_message (conn.conn))}
        conn.in_flight.extend (pending_sqls.drain (..));}}

    for conn in good_connections.iter_mut() {
      if !conn.in_flight.is_empty() {
        let sock = unsafe {pq::PQsocket (conn.conn)};

        let rc = unsafe {pq::PQconsumeInput (conn.conn)};
        if rc != 1 {panic! ("!PQconsumeInput: {}", error_message (conn.conn))}

        let rc = unsafe {pq::PQflush (conn.conn)};
        if rc == 1 {fds.push (PollFd::new (sock, poll::POLLOUT, EventFlags::empty()))}

        loop {
          let res = unsafe {pq::PQgetResult (conn.conn)};
          if res == null_mut() {
            assert! (conn.in_flight.is_empty(), "leftovers: {:?}", conn.in_flight);
            break}
          let value = unsafe {CStr::from_ptr (pq::PQgetvalue (res, 0, 0))} .to_str() .expect ("!value");
          let pg_result = conn.in_flight.pop_front().expect ("!pop_front");
          println! ("Got PGresult: {:?}, future: {:?}, value: '{}'.", res, pg_result, value);
          pg_result.0.res.store (res, Ordering::Relaxed);}

        fds.push (PollFd::new (sock, poll::POLLIN, EventFlags::empty()))}}

    fds.push (PollFd::new (read_end, poll::POLLIN, EventFlags::empty()));
    let rc = poll::poll (&mut fds, 100) .expect ("!poll");
    println! ("poll rc: {}.", rc);}

  for conn in good_connections {unsafe {pq::PQfinish (conn.conn)}}
  for conn in pending_connections {unsafe {pq::PQfinish (conn)}}}

/// A cluster of several replicated PostgreSQL nodes.
pub struct Cluster {
  /// Runs the event loop.
  thread: Option<JoinHandle<()>>,
  /// Channel to the `thread`.
  tx: Sender<Message>,
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
      tx: tx,
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
  pub fn connect (&mut self, dsn: String, mul: u8) -> Result<(), String> {
    try_s! (self.tx.send (Message::Connect (dsn, mul)));
    try_s! (nix::unistd::write (self.write_end, &[1]));
    Ok(())}

  /// Schedule the SQL to be executed on one of the nodes.
  ///
  /// The client must ensure that no more than a single command is passed.
  /// Passing multiple queries "SELECT 1; SELECT 2" is *not* allowed.
  pub fn execute (&mut self, sql: String) -> Result<PgResult, String> {
    self.command_num.compare_and_swap (u64::max_value(), 0, Ordering::Relaxed);  // Recycle the set of identifiers.
    let id = self.command_num.fetch_add (1, Ordering::Relaxed) + 1;
    let pg_result = PgResult::new (id, sql);

    try_s! (self.tx.send (Message::Execute (pg_result.clone())));
    try_s! (nix::unistd::write (self.write_end, &[2]));
    Ok (pg_result)}}

impl Drop for Cluster {
  fn drop (&mut self) {
    let _ = self.tx.send (Message::Drop);
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
  use libc::{c_char, c_int, c_uint};

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
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQGETVALUE
    pub fn PQgetvalue (res: *const PGresult, tup_num: c_int, field_num: c_int) -> *mut c_char;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQGETLENGTH
    pub fn PQgetlength (res: *const PGresult, tup_num: c_int, field_num: c_int) -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQGETISNULL
    pub fn PQgetisnull (res: *const PGresult, tup_num: c_int, field_num: c_int) -> c_int;
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
