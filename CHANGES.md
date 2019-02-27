#### Version 2.8
 * Improve logging around persist failure and subsequent successful retry.
 
#### Version 2.7
 * Fix issue whereby timeout exceptions were not properly detected and persist operations not retried.

#### Version 2.6
 * Add influx persist outcome listener and failure retry logic.
 * Point.toJson() escape handling.

#### Version 2.5
 * Set all HTTP timeout values (default 15 seconds for connect/read/write).

#### Version 2.4
 * Support more detailed logging on insertion failure.

#### Version 2.3
 * Add support to query return results with timestamps in Unix epoch format with a specific precision.
 * Throw exceptions for invalid points at the time of the write() call, on the user's thread, rather
   than during serialization on the async writer thread (where the user can't catch it).

#### Version 2.2
 * Minor cleanup.  Connection timeout handling and properly closing OkHttp response resources.

#### Version 2.1
 * Use HTTP GET instead of POST for simple commands

#### Version 2.0
 * Removed use of sun.misc.Unsafe
 * Added support for queries (driver is no longer write-only)

#### Version 1.9
 * Added JSON serialization to ``Point``.  Note that not all InfluxDB measurement names, tag names, or field
   names are valid  JSON property names. In these cases, the JSON serializer will replace invalid characters
   with underscores.

#### Version 1.8
 * Add efficient accessors for Point field inspection.

#### Version 1.7
 * Add API for creating retention policies.

#### Version 1.6
 * JavaDoc and unit test fixes.

#### Version 1.5
 * Add various null checks to avoid NPEs.

#### Version 1.4
 * Fix cross-Point state corruption after copy operation.

#### Version 1.3
 * Add some JavaDoc and two copy() method variants to the Point class.

#### Version 1.2
 * Add timestamp accessor to ``Point``.

#### Version 1.1
 * Ensure max pool size is always greater than or equal to the initial pool size.

#### Version 1.0
 * Initial release.
