// cf. https://gist.github.com/ArtemGr/179fb669fa1b065fd2c11a6fd474da8a
// libpq is "too simple-minded" to pipeline on it's own: https://www.postgresql.org/message-id/26269.1356282651@sss.pgh.pa.us
// NB: There is a work in progress on a better pipelining support in libpq: https://commitfest.postgresql.org/10/634/
//     http://2ndquadrant.github.io/postgres/libpq-batch-mode.html

#![feature(type_ascription)]

#[macro_use] extern crate gstuff;
#[macro_use] extern crate lazy_static;
extern crate libc;
extern crate nix;

use libc::c_int;
use nix::poll::{self, EventFlags, PollFd};
use nix::fcntl::{O_NONBLOCK, O_CLOEXEC};
use std::ffi::{CString, CStr};
use std::ptr::null_mut;
use std::sync::mpsc::{channel, Sender, Receiver, TryRecvError};
use std::thread::JoinHandle;

#[cfg(test)] mod tests;

#[derive(Debug)]
enum Message {
  Connect (String),
  Execute (String),
  Drop}

#[derive(Debug)]
struct Connection {
  conn: *mut pq::PGconn,
  /// `PQsendQuery` in flight
  busy: bool}

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
        Message::Connect (dsn) => {
          let dsn = CString::new (dsn) .expect ("!dsn");
          let conn = unsafe {pq::PQconnectStart (dsn.as_ptr())};
          if conn == null_mut() {panic! ("!PQconnectStart")}
          pending_connections.push (conn);},
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
        good_connections.push (Connection {conn: conn, busy: false});
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

      if let Some (conn) = good_connections.iter_mut().find (|conn| !conn.busy) {
        // Join all the queries into a single batch.
        let sql = pending_sqls.join ("; ");
        println! ("SQL batch: {}", sql);
        let sql = CString::new (sql) .expect ("!sql");
        let rc = unsafe {pq::PQsendQuery (conn.conn, sql.as_ptr())};
        if rc == 0 {panic! ("!PQsendQuery: {}", error_message (conn.conn))}
        pending_sqls.clear();
        conn.busy = true;}}

    for conn in good_connections.iter_mut() {
      if conn.busy {
        let sock = unsafe {pq::PQsocket (conn.conn)};

        let rc = unsafe {pq::PQconsumeInput (conn.conn)};
        if rc != 1 {panic! ("!PQconsumeInput: {}", error_message (conn.conn))}

        let rc = unsafe {pq::PQflush (conn.conn)};
        if rc == 1 {fds.push (PollFd::new (sock, poll::POLLOUT, EventFlags::empty()))}

        loop {
          let res = unsafe {pq::PQgetResult (conn.conn)};
          if res == null_mut() {
            conn.busy = false;
            break}
          println! ("Got PGresult: {:?}.", res);
          unsafe {pq::PQclear (res)};}

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
  write_end: c_int}

impl Cluster {
  /// Start the thread.
  pub fn new() -> Result<Cluster, String> {
    let (tx, rx) = channel();
    let (read_end, write_end) = try_s! (nix::unistd::pipe2 (O_NONBLOCK | O_CLOEXEC));
    let thread = try_s! (std::thread::Builder::new().name ("pg_async".into()) .spawn (move || event_loop (rx, read_end)));
    Ok (Cluster {
      thread: Some (thread),
      tx: tx,
      write_end: write_end})}

  /// Setup a database connection to a [replicated] node.
  ///
  /// Replicated clusters have several nodes usually. This method should be used for each node.
  ///
  /// Nodes can be added on the go. E.g. add a first node, fire some queries, add a second node.
  ///
  /// * `dsn` - A keyword=value [connection string](https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-CONNSTRING).
  pub fn connect (&mut self, dsn: String) -> Result<(), String> {
    try_s! (self.tx.send (Message::Connect (dsn)));
    try_s! (nix::unistd::write (self.write_end, &[1]));
    Ok(())}

  /// Schedule the SQL to be executed on one of the nodes.
  ///
  /// Note that the SQL might have several queries, e.g. "SELECT 1; SELECT 2".
  pub fn execute (&mut self, sql: String) -> Result<(), String> {
    try_s! (self.tx.send (Message::Execute (sql)));
    try_s! (nix::unistd::write (self.write_end, &[2]));
    Ok(())}}

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
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQCLEAR
    pub fn PQclear (result: *mut PGresult);
    /// https://www.postgresql.org/docs/9.4/static/libpq-status.html#LIBPQ-PQERRORMESSAGE
    pub fn PQerrorMessage (conn: *const PGconn) -> *const c_char;
} }
