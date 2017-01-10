# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](http://semver.org/).

## [0.3.2] - 2017-01-10
###
- JSON conversion supports enums by stringifying any ASCII user types.
- Patreon badge. In case you want to thank me or help me stay afloat and develop this driver.

## [0.3.1] - 2016-12-31
### Added
- Print the error details when the pipeline SQL fails.
- Method `PgResult::fname` to get the name of a column.
- Method `PgRow::num` to get the number of a row.
- Errors related to JSON conversions can now be encoded in `PgFutureErr`.
- `PgResult::to_json` converts the query results into a JSON structure.
