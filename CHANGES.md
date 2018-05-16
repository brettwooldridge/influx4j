#### Version 1.9
 * Added JSON serialization to ``Point``.  Note that not all InfluxDB measurement names, tag names, or field names are valid JSON property
 names. In these cases, the JSON serializer will replace invalid characters with underscores.

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
