// cf. https://gist.github.com/ArtemGr/179fb669fa1b065fd2c11a6fd474da8a
// libpq is "too simple-minded" to pipeline on it's own: https://www.postgresql.org/message-id/26269.1356282651@sss.pgh.pa.us
// NB: There is a work in progress on a better pipelining support in libpq: https://commitfest.postgresql.org/10/634/
//     http://2ndquadrant.github.io/postgres/libpq-batch-mode.html

#![feature(type_ascription, integer_atomics, custom_derive)]

extern crate futures;
#[macro_use] extern crate gstuff;
extern crate inlinable_string;
extern crate itertools;
#[cfg(test)] #[macro_use] extern crate lazy_static;
extern crate libc;
extern crate nix;
extern crate serde;
#[cfg(test)] #[macro_use] extern crate serde_derive;
extern crate serde_json;

use futures::{Future, Poll, Async};
use futures::task::{park, Task};
use gstuff::now_float;
use inlinable_string::InlinableString;
use libc::{c_char, c_int, c_void, size_t};
use nix::poll::{self, EventFlags, PollFd};
use nix::fcntl::{O_NONBLOCK, O_CLOEXEC};
use serde::de::DeserializeOwned;
use serde_json::{self as json, Value as Json};
use std::collections::VecDeque;
use std::convert::From;
use std::error::Error;
use std::ffi::{CString, CStr};
use std::fmt;
use std::io;
use std::ptr::null_mut;
use std::slice::from_raw_parts;
use std::str::{from_utf8_unchecked, from_utf8, Utf8Error};
use std::sync::{Arc, Mutex, PoisonError};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver, SendError, TryRecvError};
use std::thread::JoinHandle;

#[cfg(test)] mod tests;

/// A part of an SQL query text. The query is constructed by adding `Plain` pieces "as is" and escaping the `Literal` pieces.
///
/// For example, a properly escaped "SELECT 'foo'" might be generated with
///
/// ```
///     use pg_async::PgQueryPiece::{Static as S, Literal as L};
///     vec![S("SELECT "), L("foo".into)]
/// ```
#[derive(Debug)]
pub enum PgQueryPiece {
  /// Static strings are included in the query "as is".
  Static (&'static str),
  /// Plain strings are included in the query "as is".
  Plain (String),
  /// Literals are escaped with `PQescapeLiteral` (which also places them into the single quotes).
  Literal (String),
  /// Literals are escaped with `PQescapeLiteral` (which also places them into the single quotes).
  ///
  /// InlLiteral allows for small string optimization.
  InlLiteral (InlinableString),
  /// Binary data, escaped with `PQescapeByteaConn`. Single quotes aren't added by the escape.
  Bytea (Vec<u8>)}

/// Affects the placement of the operation.
#[derive (Debug)]
pub enum PgSchedulingMode {
  /// The operation can be run on any connection, retried and rescheduled to another connection.
  AnythingGoes,
  /// The op must only run on the connection with the given number (0-based).
  /// If the connection is not available (the server is down or inaccessible) then the operation might stay in the queue forever.
  PinToConnection (u8)}

impl Default for PgSchedulingMode {
  fn default() -> PgSchedulingMode {
    PgSchedulingMode::AnythingGoes}}

/// An atomic operation that is to be asynchronously executed.
#[derive (Default)]
pub struct PgOperation {
  /// Parts of SQL text, some of which are to be escaped and some are not.
  ///
  /// Might contain several SQL statements (the number of statements MUST match the number in the `statements` field).
  pub query_pieces: Vec<PgQueryPiece>,

  /// The number of separate top-level SQL statements withing `query_pieces`.  
  /// E.g. the number of results PostgreSQL will return.
  pub statements: u32,

  /// Affects the placement of the operation.
  pub scheduling: PgSchedulingMode,

  /// Callback which fires when the SQL is escaped.  
  /// Useful for debugging. Escaped SQL can be copy-pasted "as is" into a third-party SQL client.
  ///
  ///         let query = vec! [S ("UPDATE foo SET bar = "), L (user_input)];
  ///         let mut op: PgOperation = query.into_query_pieces();
  ///         if debug {op.on_escape = Some (Box::new (|sql| log! ("escaped query: {}", sql)))}
  ///         let f = cluster.execute (op);
  pub on_escape: Option<Box<Fn(&str) + Send + Sync + 'static>>,

  /// The time, in seconds since UNIX epoch, with fractions, when the operation SHOULD timeout.
  ///
  /// If the timeout is *negative* then it only affects the scheduling (TBD).  
  /// If, on the other hand, the timeout is *positive*
  /// then "SET statement_timeout" is used on the database side to prevent the operation from taking too long.
  ///
  /// Since timeouts affect a whole statement, operations with a positive timeout are never pipelined!
  pub timeouts_at: f64}

impl fmt::Debug for PgOperation {
  fn fmt (&self, fm: &mut fmt::Formatter) -> fmt::Result {
    write! (fm, "PgOperation {{query_pieces: {:?}, statements: {}, scheduling: {:?}, on_escape: {}}}",
      self.query_pieces, self.statements, self.scheduling, if self.on_escape.is_some() {"Some"} else {"None"})}}

/// Converts the `fn execute` input into a vector of query pieces.
pub trait IntoQueryPieces {
  /// Returns the number of SQL statements (the library must know how many results to expect) and the list of pieces to escape and join.
  fn into_query_pieces (self) -> PgOperation;}

impl IntoQueryPieces for String {
  fn into_query_pieces (self) -> PgOperation {
    PgOperation {statements: 1, query_pieces: vec! [PgQueryPiece::Plain (self)], ..Default::default()}}}

impl IntoQueryPieces for &'static str {
  fn into_query_pieces (self) -> PgOperation {
    PgOperation {statements: 1, query_pieces: vec! [PgQueryPiece::Static (self)], ..Default::default()}}}

impl IntoQueryPieces for Vec<PgQueryPiece> {
  fn into_query_pieces (self) -> PgOperation {
    PgOperation {statements: 1, query_pieces: self, ..Default::default()}}}

impl IntoQueryPieces for (u32, &'static str) {
  fn into_query_pieces (self) -> PgOperation {
    PgOperation {statements: self.0, query_pieces: vec! [PgQueryPiece::Static (self.1)], ..Default::default()}}}

impl IntoQueryPieces for (u32, String) {
  fn into_query_pieces (self) -> PgOperation {
    PgOperation {statements: self.0, query_pieces: vec! [PgQueryPiece::Plain (self.1)], ..Default::default()}}}

impl IntoQueryPieces for (u32, Vec<PgQueryPiece>) {
  fn into_query_pieces (self) -> PgOperation {
    PgOperation {statements: self.0, query_pieces: self.1, ..Default::default()}}}

impl IntoQueryPieces for (u32, Vec<PgQueryPiece>, f32) {
  fn into_query_pieces (self) -> PgOperation {
    PgOperation {statements: self.0, query_pieces: self.1, timeouts_at: now_float() + self.2 as f64, ..Default::default()}}}

impl IntoQueryPieces for PgOperation {
  fn into_query_pieces (self) -> PgOperation {self}}

pub struct PgRow<'a> (&'a PgResult, u32);

impl<'a> PgRow<'a> {
  /// True if the value is NULL.
  ///
  /// * `column` - The number of the column which value is to be tested with `PQgetisnull`. 0-based.
  pub fn is_null (&self, column: u32) -> bool {
    if column > self.0.columns {panic! ("Column index {} is out of range (0..{})", column, self.0.columns)}
    1 == unsafe {pq::PQgetisnull ((self.0).res, self.1 as c_int, column as c_int)}}

  /// PostgreSQL internal OID number of the column type.
  ///
  /// * `column` - The number of the column which type is to be retrieved with `PQftype`. 0-based.
  pub fn ftype (&self, column: u32) -> pq::Oid {
    if column > self.0.columns {panic! ("Column index {} is out of range (0..{})", column, self.0.columns)}
    unsafe {pq::PQftype ((self.0).res, column as c_int)}}

  /// Returns the column name associated with the given column number. Column numbers start at 0.
  ///
  /// * `column` - The number of the column. 0-based.
  pub fn fname (&'a self, column: u32) -> Result<&'a str, Utf8Error> {
    self.0.fname (column)}

  /// Return this row's number withing the result.
  pub fn num (&self) -> u32 {self.1}

  /// Returns the number of columns in the row. Row numbers start at 0.
  pub fn len (&self) -> u32 {self.0.columns}

  /// Get the byte slice of a value in the given column.
  ///
  /// Returns an empty array if the value is NULL.
  ///
  /// * `column` - The column number. 0-based.
  pub fn col (&self, column: u32) -> &'a [u8] {
    if column > self.0.columns {panic! ("Column index {} is out of range (0..{})", column, self.0.columns)}
    let len = unsafe {pq::PQgetlength ((self.0).res, self.1 as c_int, column as c_int)};
    let val = unsafe {pq::PQgetvalue ((self.0).res, self.1 as c_int, column as c_int)};
    unsafe {from_raw_parts (val as *const u8, len as usize)}}

  /// Tries to get an UTF-8 slice of a value in the given column.
  ///
  /// Returns an empty string if the value is NULL.
  ///
  /// * `column` - The column number. 0-based.
  pub fn col_str (&self, column: u32) -> Result<&'a str, std::str::Utf8Error> {
    if column > self.0.columns {panic! ("Column index {} is out of range (0..{})", column, self.0.columns)}
    unsafe {CStr::from_ptr (pq::PQgetvalue ((self.0).res, self.1 as c_int, column as c_int))} .to_str()}

  /// Binary data unescaped from a bytea column.
  ///
  /// * `column` - The column number. 0-based.
  pub fn bytea (&self, column: u32) -> Vec<u8> {
    if column > self.0.columns {panic! ("Column index {} is out of range (0..{})", column, self.0.columns)}
    let mut len: size_t = 0;
    let mut vec = Vec::new();
    unsafe {
      let value = pq::PQgetvalue ((self.0).res, self.1 as c_int, column as c_int);
      let bytes = pq::PQunescapeBytea (value as *const u8, &mut len);
      if bytes != null_mut() {
        vec.reserve_exact (len);
        vec.extend_from_slice (from_raw_parts (bytes, len));
        pq::PQfreemem (bytes as *mut c_void);}}
    vec}

  /// Converts the column into JSON.
  ///
  /// * `column` - The number of the column which value is to be converted to JSON. 0-based.
  /// * `name` - The name of the column. To make the errors more verbose if they'd happen.
  pub fn col_json (&self, column: u32, name: &str) -> Result<Json, PgFutureErr> {
    Ok (if self.is_null (column) {
      Json::Null
    } else {
      match self.ftype (column) {  // cf. "/usr/include/postgresql/9.*/server/catalog/pg_type.h"
        16 => Json::Bool (self.col (column) == b"t"),  // 16 boolean
        18 => {  // 18 "char"
          // Funny thing is, libpq "eats" the zero character, turns it into an empty string.
          let slice = self.col (column);
          Json::Number ((if slice.is_empty() {0} else {slice[0]}) .into())},
        20 | 21 | 23 => Json::Number ((self.col_str (column) ?.parse()? :i64).into()),  // 20 bigint, 21 smallint, 23 integer
        25 | 1042 | 1043 | 3614 => Json::String (from_utf8 (self.col (column)) ?.into()),  // 25 text, 1042 char, 1043 varchar, 3614 tsvector
        114 | 3802 => json::from_slice (self.col (column)) ?,  // 114 json, 3802 jsonb
        700 | 701 => {  // 700 real, 701 double precision
          let f: f64 = self.col_str (column) ?.parse()?;
          Json::Number (json::Number::from_f64 (f) .ok_or ("The float is not a JSON number") ?)},
        705 => Json::String (from_utf8 (self.col (column)) ?.into()),  // 705 unknown. SELECT 'foo'.
        // TODO (types I use):
        // 1184 => Type::TimestampTZ,
        // 1700 => Type::Numeric,
        // 3926 => Type::Int8Range
        oid if oid > 16000 => {
          // OID that large belong to user-defined types.
          // We support enums by stringifying all user types in case they are ASCII.
          // (I wonder if we could use `#define ANYENUMOID 3500` somehow to better detect if the OID is enum).
          let bytes = self.col (column);
          if bytes.is_empty() {return Err (PgFutureErr::UnknownType (String::from (name), oid))}
          for &ch in bytes.iter() {
            if !is_alphabetic (ch) && !is_digit (ch) && ch != b'_' && ch != b'-' && ch != b'.' {
              return Err (PgFutureErr::UnknownType (String::from (name), oid))}}
          Json::String (unsafe {from_utf8_unchecked (bytes)} .into())},
        oid => return Err (PgFutureErr::UnknownType (String::from (name), oid))}})}

  /// Auto-unpack the column value.
  pub fn col_deserialize<T: DeserializeOwned> (&self, column: u32, name: &str) -> Result<T, PgFutureErr> {
    if !self.is_null (column) {
      match self.ftype (column) {
        114 | 3802 =>  // 114 json, 3802 jsonb
          return Ok (json::from_slice (self.col (column)) ?),  // Shortcut to `from_slice`, avoiding intermediate Json representation.
        _ => ()}}

    Ok (json::from_value (self.col_json (column, name) ?) ?)}

  /// Converts the row into JSON, {$name: $value, ...}.
  pub fn to_json (&self) -> Result<Json, PgFutureErr> {
    let mut jrow: json::Map<String, Json> = json::Map::new();
    for column in 0 .. self.0.columns {
      let name = self.fname (column) ?;
      let jval = self.col_json (column, name) ?;
      jrow.insert (String::from (name), jval);}
    Ok (Json::Object (jrow))}}

fn is_alphabetic (ch:u8) -> bool {
  (ch >= 0x41 && ch <= 0x5A) || (ch >= 0x61 && ch <= 0x7A)}

fn is_digit (ch: u8) -> bool {
  ch >= 0x30 && ch <= 0x39}

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

  /// Returns the column name associated with the given column number. Column numbers start at 0.
  pub fn fname<'a> (&'a self, column: u32) -> Result<&'a str, Utf8Error> {
    let name = unsafe {pq::PQfname (self.res, column as c_int)};
    if name == null_mut() {panic! ("Column index {} is out of range (0..{})", column, self.columns)}
    unsafe {CStr::from_ptr (name)} .to_str()}

  pub fn row (&self, row: u32) -> PgRow {
    if row >= self.rows {panic! ("Row index {} is out of range (0..{})", row, self.rows)}
    PgRow (self, row)}

  /// Iterator over result rows.
  pub fn iter<'a> (&'a self) -> PgResultIt<'a> {
    PgResultIt {pr: self, row: 0}}

  /// Converts a PostgreSQL query result into a JSON array of rows, [{$name: $value, ...}, ...].
  pub fn to_json (&self) -> Result<Json, PgFutureErr> {
    let mut jrows: Vec<Json> = Vec::with_capacity (self.len() as usize);
    for row in self.iter() {jrows.push (row.to_json()?)}
    Ok (Json::Array (jrows))}

  /// Auto-unpack results.
  ///
  /// ```
  /// #[derive(Deserialize)] struct Bar {id: i64}
  /// impl Bar {
  ///   fn load() -> Box<Future<Item=Vec<Bar>, Error=PgFutureErr>> {
  ///     Box::new (PGA.execute ("SELECT id FROM bars") .and_then (|pr| pr[0].deserialize()))
  ///   }
  /// }
  /// ```
  pub fn deserialize<T: DeserializeOwned> (&self) -> Result<Vec<T>, PgFutureErr> {
    let mut jrows: Vec<T> = Vec::with_capacity (self.len() as usize);
    for row in self.iter() {jrows.push (json::from_value (row.to_json()?) ?)}
    Ok (jrows)}}

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
  op: PgOperation,
  sync: Mutex<PgFutureSync>,
  miscarried: Option<Box<PgFutureErr>>}

/// The part of `PgFutureErr` that's used when the SQL fails.
#[derive(Clone)]
pub struct PgSqlErr {
  /// Pointer to the failed future, which we keep in order to print the SQL in `Debug`.
  imp: Arc<PgFutureImpl>,
  /// Which statement has failed.
  ///
  /// In a single-statement ops this is usually `0` for the statement itself or `1` for the COMMIT.
  pub num: u32,
  /// Returned by `PQresStatus`, "string constant describing the status code".
  pub status: &'static str,
  /// The SQLSTATE code identifies the type of error that has occurred;
  /// it can be used by front-end applications to perform specific operations (such as error handling) in response to a particular database error.
  /// cf. https://www.postgresql.org/docs/9.4/static/errcodes-appendix.html
  pub sqlstate: String,
  pub message: String}

/// Returned when the future fails. Might print the SQL in `Debug`.
#[derive(Clone)]
pub enum PgFutureErr {
  PoisonError,
  SendError (String),
  NixError (nix::Error),
  Utf8Error (Utf8Error),
  Sql (PgSqlErr),
  Json (Arc<serde_json::Error>),
  Int (std::num::ParseIntError),
  Float (std::num::ParseFloatError),
  /// Happens when we don't know how to convert a value to JSON.
  UnknownType (String, pq::Oid),
  Str (&'static str)}

impl PgFutureErr {
  /// True if the future was likely to have been terminated due to a PostgreSQL timeout.
  pub fn pg_timeout (&self) -> bool {
    if let &PgFutureErr::Sql (ref pg_sql_err) = self {
      if pg_sql_err.sqlstate == "57014" {
        return true}}
    false}}

impl fmt::Debug for PgFutureErr {
  fn fmt (&self, ft: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    match self {
      &PgFutureErr::PoisonError => ft.write_str ("PgFutureErr::PoisonError"),
      &PgFutureErr::SendError (ref err) => write! (ft, "PgFutureErr::SendError ({})", err),
      &PgFutureErr::NixError (ref err) => write! (ft, "PgFutureErr::NixError ({:?})", err),
      &PgFutureErr::Utf8Error (ref err) => write! (ft, "PgFutureErr::Utf8Error ({:?})", err),
      &PgFutureErr::Sql (ref se) => write! (ft, "PgFutureErr ({:?}, {}, {})", se.imp.op.query_pieces, se.sqlstate, se.message),
      &PgFutureErr::Json (ref err) => write! (ft, "PgFutureErr::Json ({:?})", err),
      &PgFutureErr::Int (ref err) => write! (ft, "PgFutureErr::Int ({:?})", err),
      &PgFutureErr::Float (ref err) => write! (ft, "PgFutureErr::Float ({:?})", err),
      &PgFutureErr::UnknownType (ref fname, oid) => write! (ft, "PgFutureErr::UnknownType (fname '{}', oid {})", fname, oid),
      &PgFutureErr::Str (s) => ft.write_str (s)}}}

impl fmt::Display for PgFutureErr {
  fn fmt (&self, ft: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    match self {
      &PgFutureErr::PoisonError => ft.write_str ("PgFutureErr::PoisonError"),
      &PgFutureErr::SendError (ref err) => write! (ft, "PgFutureErr::SendError ({})", err),
      &PgFutureErr::NixError (ref err) => write! (ft, "PgFutureErr::NixError ({})", err),
      &PgFutureErr::Utf8Error (ref err) => write! (ft, "PgFutureErr::Utf8Error ({})", err),
      &PgFutureErr::Sql (ref se) => write! (ft, "{}; {}", se.sqlstate, se.message),
      &PgFutureErr::Json (ref err) => write! (ft, "PgFutureErr::Json ({})", err),
      &PgFutureErr::Int (ref err) => write! (ft, "PgFutureErr::Int ({})", err),
      &PgFutureErr::Float (ref err) => write! (ft, "PgFutureErr::Float ({})", err),
      &PgFutureErr::UnknownType (ref fname, oid) => write! (ft, "Column '{}' has unfamiliar OID {}", fname, oid),
      &PgFutureErr::Str (s) => ft.write_str (s)}}}

impl Error for PgFutureErr {
  fn description (&self) -> &str {
    match self {
      &PgFutureErr::PoisonError => "PgFutureErr::PoisonError",
      &PgFutureErr::SendError (_) => "PgFutureErr::SendError",
      &PgFutureErr::NixError (_) => "PgFutureErr::NixError",
      &PgFutureErr::Utf8Error (_) => "PgFutureErr::Utf8Error",
      &PgFutureErr::Sql (ref se) => &se.message[..],
      &PgFutureErr::Json (_) => "PgFutureErr::Json",
      &PgFutureErr::Int (_) => "PgFutureErr::Int",
      &PgFutureErr::Float (_) => "PgFutureErr::Float",
      &PgFutureErr::UnknownType (..) => "PgFutureErr::UnknownType",
      &PgFutureErr::Str (s) => s}}}

impl<T> From<PoisonError<T>> for PgFutureErr {
  fn from (_err: PoisonError<T>) -> PgFutureErr {
    PgFutureErr::PoisonError}}

impl From<Utf8Error> for PgFutureErr {
  fn from (err: Utf8Error) -> PgFutureErr {
    PgFutureErr::Utf8Error (err)}}

impl From<serde_json::Error> for PgFutureErr {
  fn from (err: serde_json::Error) -> PgFutureErr {
    PgFutureErr::Json (Arc::new (err))}}

impl From<std::num::ParseIntError> for PgFutureErr {
  fn from (err: std::num::ParseIntError) -> PgFutureErr {
    PgFutureErr::Int (err)}}

impl From<std::num::ParseFloatError> for PgFutureErr {
  fn from (err: std::num::ParseFloatError) -> PgFutureErr {
    PgFutureErr::Float (err)}}

impl<T> From<SendError<T>> for PgFutureErr {
  fn from (err: SendError<T>) -> PgFutureErr {
    PgFutureErr::SendError (format! ("{}", err))}}

impl From<nix::Error> for PgFutureErr {
  fn from (err: nix::Error) -> PgFutureErr {
    PgFutureErr::NixError (err)}}

impl From<&'static str> for PgFutureErr {
  fn from (err: &'static str) -> PgFutureErr {
    PgFutureErr::Str (err)}}

/// Delayed SQL result.
#[derive(Clone)]
pub struct PgFuture (Arc<PgFutureImpl>);

impl PgFuture {
  fn new (id: u64, op: PgOperation) -> PgFuture {
    PgFuture (Arc::new (PgFutureImpl {
      id: id,
      op: op,
      sync: Mutex::new (PgFutureSync {
        results: Vec::new(),
        task: None}),
      miscarried: None}))}

  fn error (err: PgFutureErr) -> PgFuture {
    PgFuture (Arc::new (PgFutureImpl {
      id: 0,
      op: PgOperation::default(),
      sync: Mutex::new (PgFutureSync {
        results: Vec::new(),
        task: None}),
      miscarried: Some (Box::new (err))}))}}

impl fmt::Debug for PgFuture {
  fn fmt (&self, ft: &mut fmt::Formatter) -> Result<(), fmt::Error> {
    if let Some (ref err) = self.0.miscarried {
      write! (ft, "PgFuture ({:?})", err)
    } else {
      write! (ft, "PgFuture ({}, {:?})", self.0.id, self.0.op)}}}

impl Future for PgFuture {
  type Item = Vec<PgResult>;
  type Error = PgFutureErr;
  fn poll (&mut self) -> Poll<Vec<PgResult>, PgFutureErr> {
    if let Some (ref err) = self.0.miscarried {return Err (*err.clone())}

    let mut sync = self.0.sync.lock()?;

    if sync.results.is_empty() {
      sync.task = Some (park());
      return Ok (Async::NotReady)}

    for (pr, num) in sync.results.iter().zip (0..) {
      if let Some (status) = error_in_result (pr.res) {
        let status = unsafe {CStr::from_ptr (pq::PQresStatus (status))} .to_str()?;
        let sqlstate = unsafe {CStr::from_ptr (pq::PQresultErrorField (pr.res, pq::PG_DIAG_SQLSTATE))} .to_str()?;
        let err = unsafe {CStr::from_ptr (pq::PQresultErrorMessage (pr.res))} .to_str()?;
        return Err (PgFutureErr::Sql (PgSqlErr {
          imp: self.0.clone(),
          num: num,
          status: status,
          sqlstate: sqlstate.into(),
          message: format! ("Error in statement {}: {}; {}", num, status, err)}))}}

    let mut res = Vec::with_capacity (sync.results.len());
    res.append (&mut sync.results);

    Ok (Async::Ready (res))}}

#[derive(Debug)]
enum Message {
  Connect (String, u8),
  Execute (PgFuture),
  /// Tells the event loop to emulate an error. Is only effective in the test builds.
  EmulateErrorAt (u8, String),
  /// Stops the event loop.
  Drop}

#[derive(Debug)]
struct Connection {
  /// Keep connection string, allowing us to reconnect when connection fails.
  dsn: String,
  handle: *mut pq::PGconn,
  in_flight: VecDeque<PgFuture>,
  /// Incoming futures pinned to this particular connection.
  pinned_pending_futures: VecDeque<PgFuture>,
  /// True if we're waiting for the server connection to happen.
  connection_pending: bool,
  /// True if there is an internal statement in flight.
  in_flight_init: bool,
  /// True if statement_timeout was previously used. If it was, then we'd need to reset it back to 0.
  statement_timeout: bool}

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

// To reconnect or to panic, that is the question...
fn reconnect_heuristic (err: &str) -> bool {
  err.starts_with ("server closed the connection unexpectedly") ||
  err.starts_with ("SSL SYSCALL error")}

fn event_loop (rx: Receiver<Message>, read_end: c_int) {
  // NB: Connection must not leak into other threads.
  // "One thread restriction is that no two threads attempt to manipulate the same PGconn object at the same time.
  // In particular, you cannot issue concurrent commands from different threads through the same connection object.
  // (If you need to run concurrent commands, use multiple connections.)"
  use std::fmt::Write;

  let mut connections: Vec<Connection> = Vec::new();
  let mut pending_futures: VecDeque<PgFuture> = VecDeque::new();
  let mut sql = String::with_capacity (PIPELINE_LIM_BYTES + 1024);
  let mut sql_futures = Vec::with_capacity (PIPELINE_LIM_COMMANDS);
  let mut error_at: u8 = 0;  // Hardcoded identifier of a place where an error should be emulated.
  let mut error_at_message = String::new();  // The error message to emulate at `error_at`.

  macro_rules! schedule {($pg_future: ident) => {{
    match $pg_future.0.op.scheduling {
      PgSchedulingMode::AnythingGoes => pending_futures.push_back ($pg_future),
      PgSchedulingMode::PinToConnection (n) => connections[n as usize].pinned_pending_futures.push_back ($pg_future)}}}};

  macro_rules! reschedule {($conn: ident, $pg_future: ident) => {{
    match $pg_future.0.op.scheduling {
      PgSchedulingMode::AnythingGoes => pending_futures.push_back ($pg_future),
      PgSchedulingMode::PinToConnection (_) => $conn.pinned_pending_futures.push_back ($pg_future)}}}};

  let mut fds = Vec::new();
  'event_loop: loop {
    { let mut tmp: [u8; 256] = unsafe {std::mem::uninitialized()}; let _ = nix::unistd::read (read_end, &mut tmp); }
    loop {match rx.try_recv() {
      Err (TryRecvError::Disconnected) => break 'event_loop,  // Cluster was dropped.
      Err (TryRecvError::Empty) => break,
      Ok (message) => match message {
        Message::Connect (dsn, mul) => {
          let dsn_c = CString::new (&dsn[..]) .expect ("!dsn");
          for _ in 0 .. mul {
            let conn = unsafe {pq::PQconnectStart (dsn_c.as_ptr())};
            if conn == null_mut() {panic! ("!PQconnectStart")}
            connections.push (Connection {
              dsn: dsn.clone(),
              handle: conn,
              in_flight: VecDeque::new(),
              pinned_pending_futures: VecDeque::new(),
              connection_pending: true,
              in_flight_init: false,
              statement_timeout: false});}},
        Message::Execute (pg_future) => schedule! (pg_future),
        Message::EmulateErrorAt (at, message) => {error_at = at; error_at_message = message},
        Message::Drop => break 'event_loop}}}  // Cluster is dropping.

    fds.clear();

    for conn in connections.iter_mut().filter (|conn| conn.connection_pending) {
      let status = unsafe {pq::PQstatus (conn.handle)};
      if status == pq::CONNECTION_BAD {
        // TODO: Log the failed connection attempt.
        // TODO: An option not to retry too fast?
        unsafe {pq::PQresetStart (conn.handle)};  // Keep trying.
      } else if status == pq::CONNECTION_OK {
        let rc = unsafe {pq::PQsetnonblocking (conn.handle, 1)};
        if rc != 0 {panic! ("!PQsetnonblocking: {}", error_message (conn.handle))}

        // Pipelining usually works by executing all queries in a single transaction, but we might want to avoid that
        // because we don't want a single failing query to invalidate (roll back) all DMLs that happen to share the same pipeline.
        // This means we might be wrapping each query in a transaction
        // and need some other means to amortize the const of many transactions being present in the pipeline.
        //
        // BDR defaults to asynchronous commits anyway, cf. http://bdr-project.org/docs/stable/bdr-configuration-variables.html#GUC-BDR-SYNCHRONOUS-COMMIT
        let rc = unsafe {pq::PQsendQuery (conn.handle, "SET synchronous_commit = off\0".as_ptr() as *const c_char)};
        if rc == 0 {panic! ("!PQsendQuery: {}", error_message (conn.handle))}

        conn.connection_pending = false;
        conn.in_flight_init = true;
      } else {
        let sock = unsafe {pq::PQsocket (conn.handle)};
        match unsafe {pq::PQconnectPoll (conn.handle)} {
          pq::PGRES_POLLING_READING => fds.push (PollFd::new (sock, poll::POLLIN, EventFlags::empty())),
          pq::PGRES_POLLING_WRITING => fds.push (PollFd::new (sock, poll::POLLOUT, EventFlags::empty())),
          _ => ()};}}

    // Try to find a connection that isn't currently busy.
    // (As of now, only one `PQsendQuery` can be in flight).
    for conn in connections.iter_mut().filter (|conn| !conn.connection_pending && conn.free()) {
      if pending_futures.is_empty() && conn.pinned_pending_futures.is_empty() {continue}

      // Join all the queries into a single batch.
      // Similar to how libpqxx pipelining works, cf.
      // https://github.com/jtv/libpqxx/blob/master/include/pqxx/pipeline.hxx
      // https://github.com/jtv/libpqxx/blob/master/src/pipeline.cxx
      sql.clear();
      sql_futures.clear();
      let mut first = true;
      let mut statement_timeout_ms: u32 = 0;
      loop {
        if sql.len() >= PIPELINE_LIM_BYTES {break}
        if sql_futures.len() + 1 >= PIPELINE_LIM_COMMANDS {break}
        if statement_timeout_ms != 0 && !first {break}
        let pending = match pending_futures.pop_front() {
          Some (f) => f,
          None => match conn.pinned_pending_futures.pop_front() {
            Some (f) => f,
            None => break}};

        // If the operation is using a statement_timeout then it must be alone and not pipelined with other operations.
        if pending.0.op.timeouts_at > 0.0 {
          if first {
            let now = now_float();
            let remains = pending.0.op.timeouts_at - now;
            statement_timeout_ms = if remains <= 0.001 {1} else {(remains * 1000.0) as u32 + 1};
            conn.statement_timeout = true;
          } else {
            pending_futures.push_front (pending);
            break}}

        if first {
          first = false;
          if statement_timeout_ms != 0 {
            write! (&mut sql, "SET statement_timeout = {}; ", statement_timeout_ms) .expect ("!write");
          } else if conn.statement_timeout {
            // NB: We shall NOT reset the `conn.statement_timeout` here because the `sql` might end being malformed.
            sql.push_str ("SET statement_timeout = 0; ")}
        } else {
          sql.push_str ("; ")}

        write! (&mut sql, "BEGIN; SELECT {} AS future_id; ", pending.0.id) .expect ("!write");

        let escape_start_len = sql.len();
        for piece in pending.0.op.query_pieces.iter() {
          match piece {
            &PgQueryPiece::Static (ref ss) => sql.push_str (ss),
            &PgQueryPiece::Plain (ref plain) => sql.push_str (&plain),
            &PgQueryPiece::Literal (ref literal) => {
              let esc = unsafe {pq::PQescapeLiteral (conn.handle, literal.as_ptr() as *const c_char, literal.len())};
              if esc == null_mut() {panic! ("!PQescapeLiteral: {}", error_message (conn.handle))}
              sql.push_str (unsafe {CStr::from_ptr (esc)} .to_str().expect ("!esc"));
              unsafe {pq::PQfreemem (esc as *mut c_void)};},
            &PgQueryPiece::InlLiteral (ref literal) => {
              let esc = unsafe {pq::PQescapeLiteral (conn.handle, literal.as_ptr() as *const c_char, literal.len())};
              if esc == null_mut() {panic! ("!PQescapeLiteral: {}", error_message (conn.handle))}
              sql.push_str (unsafe {CStr::from_ptr (esc)} .to_str().expect ("!esc"));
              unsafe {pq::PQfreemem (esc as *mut c_void)};},
            &PgQueryPiece::Bytea (ref bytes) => {
              let mut escaped_size: size_t = 0;
              let esc = unsafe {pq::PQescapeByteaConn (conn.handle, bytes.as_ptr(), bytes.len(), &mut escaped_size)};
              if esc == null_mut() {panic! ("!PQescapeByteaConn: {}", error_message (conn.handle))}
              assert! (escaped_size > 0);  // NB: Includes the terminating zero byte.
              let esc_slice = unsafe {from_raw_parts (esc, escaped_size - 1)};
              sql.push_str (unsafe {from_utf8_unchecked (esc_slice)});
              unsafe {pq::PQfreemem (esc as *mut c_void)};}}}

        if let Some (ref on_escape) = pending.0.op.on_escape {
          on_escape (&sql[escape_start_len..])}

        // Wrap every command in a separate transaction in order not to loose the DML changes when there is an erroneous statement in the pipeline.
        sql.push_str ("; COMMIT");
        sql_futures.push (pending)}

      // NB: We need this call to flip a flag in libpq, particularly when the loop that process the results finishes early.
      let res = unsafe {pq::PQgetResult (conn.handle)};
      if res != null_mut() {panic! ("Stray result detected before PQsendQuery! {:?}", res);}

      //println! ("sql: {}", sql);  // TODO: turn on debug logging in unit tests.
      let sql = CString::new (&sql[..]) .expect ("sql !CString");
      let rc = unsafe {pq::PQsendQuery (conn.handle, sql.as_ptr())};
      if rc == 0 {
        let err = error_message (conn.handle);
        if reconnect_heuristic (&err) {
          // Experimental reconnection support.
          // TODO: Log the reconnection.
          unsafe {pq::PQresetStart (conn.handle)};
          conn.connection_pending = true;
          for future in sql_futures.drain (..) {reschedule! (conn, future)}
          continue}
        panic! ("!PQsendQuery: {}", error_message (conn.handle))}
      conn.in_flight.extend (sql_futures.drain (..));}

    'connections_loop: for conn in connections.iter_mut().filter (|conn| !conn.connection_pending) {
      if !conn.free() {
        let sock = unsafe {pq::PQsocket (conn.handle)};

        let rc = unsafe {pq::PQconsumeInput (conn.handle)};
        if rc != 1 {
          let err = error_message (conn.handle);
          if reconnect_heuristic (&err) {
            // Experimental reconnection support.
            // TODO: Log the reconnection.
            unsafe {pq::PQreset (conn.handle)};
            conn.connection_pending = true;
            for future in conn.in_flight.drain (..) {reschedule! (conn, future)}
            continue 'connections_loop}
          panic! ("!PQconsumeInput: {}", err)}

        let rc = unsafe {pq::PQflush (conn.handle)};
        if rc == 1 {fds.push (PollFd::new (sock, poll::POLLOUT, EventFlags::empty()))}

        loop {
          if unsafe {pq::PQisBusy (conn.handle)} == 1 {break}
          let mut error = false;
          let mut after_future = false;  // True if we've popped a future from `conn.in_flight`.
          if conn.in_flight_init {
            conn.in_flight_init = false;
            let res = unsafe {pq::PQgetResult (conn.handle)};
            if res == null_mut() {panic! ("Got no result for in_flight_init statement")}
            error = error_in_result (res) .is_some()
          } else {
            let pg_future = match conn.in_flight.pop_front() {Some (f) => f, None => break};
            after_future = true;

            // Every statement is wrapped in a "BEGIN; SELECT $id; $statement; COMMIT" transaction and produces not one but three results.
            // If `conn.statement_timeout` is true then there is also a "SET statement_timeout = $ms;" before the first transaction.

            { let first_res = unsafe {pq::PQgetResult (conn.handle)};
              if first_res == null_mut() {panic! ("No first result for {:?}", pg_future)}
              if let Some (error_status) = error_in_result (first_res) {
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
                let sqlstate = unsafe {CStr::from_ptr (pq::PQresultErrorField (first_res, pq::PG_DIAG_SQLSTATE))} .to_str() .expect ("!to_str");
                if sqlstate == "57P01" {
                  // Experimental reconnection support.
                  // TODO: Log the reconnection.
                  unsafe {pq::PQresetStart (conn.handle)};
                  conn.connection_pending = true;
                  reschedule! (conn, pg_future);
                  for future in conn.in_flight.drain (..) {reschedule! (conn, future)}
                  continue 'connections_loop}

                let status = unsafe {CStr::from_ptr (pq::PQresStatus (error_status))} .to_str() .expect ("!to_str");
                let err = unsafe {CStr::from_ptr (pq::PQresultErrorMessage (first_res))} .to_str() .expect ("!to_str");
                panic! ("First statement failed. Probably a syntax error in one of the pipelined SQL statements. {}, {},\n{}", status, sqlstate, err);}
              unsafe {pq::PQclear (first_res)} }

            if pg_future.0.op.timeouts_at > 0.0 || conn.statement_timeout {
              if !(pg_future.0.op.timeouts_at > 0.0) {  // So the first statement was "SET statement_timeout = 0;" and it worked.
                conn.statement_timeout = false}

              let begin_res = unsafe {pq::PQgetResult (conn.handle)};
              if begin_res == null_mut() {panic! ("Got no BEGIN result for {:?}", pg_future)}
              if let Some (error_status) = error_in_result (begin_res) {
                let sqlstate = unsafe {CStr::from_ptr (pq::PQresultErrorField (begin_res, pq::PG_DIAG_SQLSTATE))} .to_str() .expect ("!to_str");
                let status = unsafe {CStr::from_ptr (pq::PQresStatus (error_status))} .to_str() .expect ("!to_str");
                let err = unsafe {CStr::from_ptr (pq::PQresultErrorMessage (begin_res))} .to_str() .expect ("!to_str");
                panic! ("BEGIN failed. {}, {},\n{}", status, sqlstate, err);}}

            let mut sync = pg_future.0.sync.lock().expect ("!lock");

            if !error { // Check that we're getting results for the right future.
              let id_res = unsafe {pq::PQgetResult (conn.handle)};
              if id_res == null_mut() {panic! ("Got no ID result for {:?}", pg_future)}
              if error_in_result (id_res) .is_some() {panic! ("Error in ID")}
              assert_eq! (unsafe {pq::PQntuples (id_res)}, 1);
              assert_eq! (unsafe {pq::PQnfields (id_res)}, 1);
              let id = unsafe {CStr::from_ptr (pq::PQgetvalue (id_res, 0, 0))} .to_str() .expect ("!to_str");
              let id: u64 = id.parse().expect ("!id");
              assert_eq! (id, pg_future.0.id);  // The check.
              unsafe {pq::PQclear (id_res)} }

            let expect_results = pg_future.0.op.statements as usize + 1;
            sync.results.reserve_exact (expect_results);
            for num in 0..expect_results {
              if error {break}
              let statement_res = unsafe {pq::PQgetResult (conn.handle)};
              if statement_res == null_mut() {panic! ("Got no statement {} result for {:?}", num, pg_future)}
              error = error_in_result (statement_res) .is_some();
              sync.results.push (PgResult {  // NB: `PgResult` takes responsibility to `PQclear` the `statement_res`.
                res: statement_res,
                rows: unsafe {pq::PQntuples (statement_res)} as u32,
                columns: unsafe {pq::PQnfields (statement_res)} as u32});}

            if let Some (ref task) = sync.task {task.unpark()}}

          if error || (cfg! (test) && error_at == 1) {
            if after_future {
              // Need to call `PQgetResult` one last time in order to flip a flag in libpq. Otherwise `PQsendQuery` won't work.
              let res = unsafe {pq::PQgetResult (conn.handle)};
              if res != null_mut() || (cfg! (test) && error_at == 1) {
                let err = if cfg! (test) && error_at == 1 {error_at_message.clone()} else {error_message (conn.handle)};
                if reconnect_heuristic (&err) {
                  // Experimental reconnection support.
                  // TODO: Log the reconnection.
                  unsafe {pq::PQresetStart (conn.handle)};
                  conn.connection_pending = true;
                  for future in sql_futures.drain (..) {reschedule! (conn, future)}
                  continue 'connections_loop}
                panic! ("Unexpected result after an error. error_message: {}", err)}

              // We're using BEGIN-COMMIT, so we should ROLLBACK after a failure.
              // Otherwise we'll see "current transaction is aborted, commands ignored until end of transaction block".
              let rc = unsafe {pq::PQsendQuery (conn.handle, "ROLLBACK\0".as_ptr() as *const c_char)};
              if rc == 0 {panic! ("!PQsendQuery: {}", error_message (conn.handle))}
              conn.in_flight_init = true}

            // If a statement fails then the rest of the pipeline fails. Reschedule the remaining futures.
            for future in conn.in_flight.drain (..) {reschedule! (conn, future)}}}

        fds.push (PollFd::new (sock, poll::POLLIN, EventFlags::empty()))}}

    fds.push (PollFd::new (read_end, poll::POLLIN, EventFlags::empty()));
    let rc = poll::poll (&mut fds, 100) .expect ("!poll");
    if rc == -1 {panic! ("!poll: {}", io::Error::last_os_error())}}

  for conn in connections {unsafe {pq::PQfinish (conn.handle)}}}

// Wakes up the event loop by writing into the pipe.
fn wake_up (write_end: c_int, payload: u8) -> Result<(), String> {
  if let Err (err) = nix::unistd::write (write_end, &[payload]) {
    match err {
      nix::Error::Sys (errno) if errno == nix::Errno::EAGAIN => (),  // So the pipe can overflow when there's a lot of wake up calls.
      _ => return ERR! ("!write on write_end: {}", err)}}
  Ok(())}

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
    try_s! (wake_up (self.write_end, 1));
    Ok(())}

  /// Schedule an SQL command to be executed on one of the nodes.
  ///
  /// SQL will be run inside a transaction.
  ///
  /// When using multiple statements, the library user must specify the exact number of top-level statements that the PostgreSQL server is going to see.
  /// For example:
  ///
  /// ```
  ///   // Running two statements as a single op:
  ///   cluster.execute ((2, "SELECT 1; SELECT 2"));
  ///   // Running three statements:
  ///   cluster.execute ((3, "\
  ///     DELETE FROM foo; \
  ///     INSERT INTO foo VALUES (1); \
  ///     INSERT INTO foo VALUES (2)"));
  /// ```
  ///
  /// To avoid SQL injection one might use the escapes provided by `PgQueryPiece`:
  ///
  /// ```
  ///   use pg_async::PgQueryPiece::{Static as S, Plain as P, Literal as L, Bytea as B};
  ///   cluster.execute (vec! [S ("SELECT * FROM foo WHERE bar = "), L (bar)]);
  /// ```
  pub fn execute<I: IntoQueryPieces> (&self, sql: I) -> PgFuture {
    self.command_num.compare_and_swap (u64::max_value(), 0, Ordering::Relaxed);  // Recycle the set of identifiers.
    let id = self.command_num.fetch_add (1, Ordering::Relaxed) + 1;
    let pg_future = PgFuture::new (id, sql.into_query_pieces());

    { let tx = match self.tx.lock() {Ok (k) => k, Err (err) => return PgFuture::error (err.into())};
      if let Err (err) = tx.send (Message::Execute (pg_future.clone())) {return PgFuture::error (err.into())}; }
    if let Err (err) = wake_up (self.write_end, 2) {panic! ("{}", err)}
    pg_future}

  /// Emulate an error. Only works in test builds. Might panic.
  ///
  /// * `at` - Identifies a hardcoded place (or a chain of places) where an [libpq] error is to be emulated.
  /// * `message` - The emulated error message.
  #[doc(hidden)]
  pub fn emulate_error_at (&self, at: u8, message: String) {
    if cfg! (test) {
      { let tx = self.tx.lock().expect ("!lock");
        tx.send (Message::EmulateErrorAt (at, message)) .expect ("!send"); }
      if let Err (err) = wake_up (self.write_end, 2) {panic! ("{}", err)}}}}

impl Drop for Cluster {
  fn drop (&mut self) {
    if let Ok (tx) = self.tx.lock() {
      let _ = tx.send (Message::Drop);}
    let _ = wake_up (self.write_end, 3);
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

  // PG_DIAG_* constants are defined in /usr/include/postgresql/postgres_ext.h

  /// According to https://www.postgresql.org/message-id/20041202060648.GA60984%40winnie.fuhr.org
  /// returns a error code from https://www.postgresql.org/docs/9.4/static/errcodes-appendix.html.
  pub const PG_DIAG_SQLSTATE: c_int = 'C' as c_int;

  #[link(name = "pq")] extern {
    /// https://www.postgresql.org/docs/9.4/static/libpq-threading.html
    pub fn PQisthreadsafe() -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-PQCONNECTDB
    pub fn PQconnectdb (conninfo: *const c_char) -> *mut PGconn;
    /// https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-PQCONNECTSTARTPARAMS
    pub fn PQconnectStart (conninfo: *const c_char) -> *mut PGconn;
    /// https://www.postgresql.org/docs/9.4/static/libpq-status.html#LIBPQ-PQSTATUS
    pub fn PQstatus (conn: *const PGconn) -> ConnStatusType;
    /// https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-PQRESET
    pub fn PQreset (conn: *mut PGconn);
    /// https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-PQRESETSTART
    pub fn PQresetStart (conn: *mut PGconn) -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-PQCONNECTSTARTPARAMS
    pub fn PQconnectPoll (conn: *mut PGconn) -> PostgresPollingStatusType;
    /// https://www.postgresql.org/docs/9.4/static/libpq-status.html#LIBPQ-PQSOCKET
    pub fn PQsocket (conn: *const PGconn) -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-async.html#LIBPQ-PQSETNONBLOCKING
    pub fn PQsetnonblocking (conn: *mut PGconn, arg: c_int) -> c_int;
    /// https://www.postgresql.org/docs/9.4/static/libpq-connect.html#LIBPQ-PQFINISH
    pub fn PQfinish (conn: *mut PGconn);
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-EXEC-ESCAPE-STRING
    pub fn PQescapeLiteral (conn: *const PGconn, str: *const c_char, len: size_t) -> *mut c_char;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQESCAPEBYTEACONN
    pub fn PQescapeByteaConn (conn: *const PGconn, from: *const u8, from_length: size_t, to_length: *mut size_t) -> *mut u8;
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQUNESCAPEBYTEA
    pub fn PQunescapeBytea (from: *const u8, to_length: *mut size_t)-> *mut u8;
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
    /// https://www.postgresql.org/docs/9.4/static/libpq-exec.html#LIBPQ-PQRESULTERRORFIELD
    pub fn PQresultErrorField (res: *const PGresult, fieldcode: c_int) -> *mut c_char;
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
    pub fn PQerrorMessage (conn: *const PGconn) -> *const c_char;}}
