# influxdb_odata

[![Build Status](https://travis-ci.org/Synergetic-Engineering/odata-influxdb.svg?branch=master)](https://travis-ci.org/Synergetic-Engineering/odata-influxdb) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This project allows you to use the [OData REST API](http://www.odata.org/) to access [InfluxDB](https://www.influxdata.com/) data.

## Requirements:


python: Currently requires Python 2.

[pyslet](https://github.com/swl10/pyslet): The OData functionality from the pyslet project is used in this project.

## Usage:

Run the following command to generate a sample config file:

`python server.py --makeSampleConfig`

Update the `dsn` in the conf file to reflect your InfluxDB server location.

You can change the hostname/port for the API server by updating `server_root` in the conf file.

Start your odata endpoint server with `python server.py`.

Point an OData browser to `http://hostname:8080/`

## Tests:

Run unit tests with `python tests.py`

## OData layout:

Upon startup, the server pulls the metadata from your InfluxDB server
(database names, measurement names, field keys, and tag keys).

Each measurement is set up as an OData table. All field keys and tag keys
from the InfluxDB database are including in the table, but can be null
depending on your InfluxDB setup.

## Filters

OData filter spec is supported, but has some limitations.

Supported operators are:

* gt (greater than, >)
* ge (greater than or equal to, >=)
* lt (less than, <)
* le (less than or equal to, <=)
* eq (equals, =)
* ne (not equal to, !=)
* and (boolean and)

## Grouping

This project currently depends on pyslet currently gives us OData 2 
support, which does not include grouping, so this project provies
a non-standard implementation of grouping operations. Because of this,
you cannot use GUI tools to form the grouping queries.

*InfluxDB requires a WHERE clause on the time field when grouping by time.*

*The release version of pyslet had a [bug](https://github.com/swl10/pyslet/issues/71) (now fixed) where you could not use
a field called "time" so use "timestamp" to refer to InfluxDB's "time" field.*

### Example queries:

#### Group by day. Aggregate the mean of each field.

Query URL:
```
/db?$filter=timestamp ge datetime'2017-01-01T00:00:00' and timestamp le datetime'2017-03-01T00:00:00'&$top=1000&groupByTime=1h&aggregate=mean

```

Resulting InfluxDB query:
```
SELECT mean(*) FROM measurement 
  WHERE time >= '2017-01-01 AND time <= '2017-03-01'
  GROUP BY time(1d) 
```

#### Group by day. Aggregate the mean of each field. Also group by all tag keys

Query URL:
```
/db?$filter=timestamp ge datetime'2017-01-01T00:00:00' and timestamp le datetime'2017-03-01T00:00:00'&$top=1000&groupByTime=1h&aggregate=mean&influxgroupby=*

```

Resulting InfluxDB query:
```
SELECT mean(*) FROM measurement 
  WHERE time >= '2017-01-01' AND time <= '2017-03-01' 
  GROUP BY *,time(1d) 
```

