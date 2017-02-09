## Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](http://semver.org/).

### [0.4.2] - 2017-02-09
`+` JSON serialization now supports PostgreSQL "boolean".

### [0.4.1] - 2017-02-01
`+` Implemented `PgResult::deserialize`, conveniently unpack query results into a Serde struct.

### [0.4.0] - 2017-01-30
`+` Simplify the client side of `fn execute` error handling by lifting the errors into the future.  
`+` JSON serialization for a single row.

### [0.3.7] - 2017-01-24
`+` Automatically reconnect after an "SSL SYSCALL error".

### [0.3.6] - 2017-01-23
`+` In `PgResult::to_json` unpack the json and jsonb PostgreSQL types.

### [0.3.5] - 2017-01-19
`+` Operations can now be pinned to a particular connection.

### [0.3.4] - 2017-01-17
`+` A way to escape binary data was added (Bytea).

### [0.3.3] - 2017-01-11
`+` Experimental reconnection support.  
`+` SQLSTATE error code is copied to `PgSqlErr` errors.

### [0.3.2] - 2017-01-10
`+` JSON conversion supports enums by stringifying any ASCII user types.  
`+` Patreon badge. In case you want to thank me or help me stay afloat and develop this driver.

### [0.3.1] - 2016-12-31
`+` Print the error details when the pipeline SQL fails.  
`+` Method `PgResult::fname` to get the name of a column.  
`+` Method `PgRow::num` to get the number of a row.  
`+` Errors related to JSON conversions can now be encoded in `PgFutureErr`.  
`+` `PgResult::to_json` converts the query results into a JSON structure.
