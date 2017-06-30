# influxdb_odata

[![Build Status](https://travis-ci.org/Synergetic-Engineering/odata-influxdb.svg?branch=master)](https://travis-ci.org/Synergetic-Engineering/odata-influxdb) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

This project allows you to use the [OData REST API](http://www.odata.org/) to access [InfluxDB](https://www.influxdata.com/) data.

This enables software such as Power BI, Excel, SAP, Drupal, and LINQPad to access InfluxDB data.
(_Note: only some of these have been tested._)

## Requirements:

python: Currently requires Python 2. Tested on latest 2.7.

[pyslet](https://github.com/swl10/pyslet): The OData functionality from the pyslet project 
is used in this project.

## Usage:

Run the following command to generate a sample config file:

`python server.py --makeSampleConfig`

Update the `dsn` in the conf file to reflect your InfluxDB server location.

You can change the hostname/port for the API server by updating 
`service_advertise_root`, `server_listen_interface`, and `server_listen_port`
in the conf file.

Start your odata endpoint server with `python server.py`.

Point an OData browser to `http://hostname:8080/`

## Production:

The recommended production deployment is as follows:

![power bi -> https proxy -> odata-influxdb -> influxdb](img/production_deployment_diagram.png "Production Diagrom")

The odata-influxdb service is stateless/sessionless. An XML file is generated upon starting the 
server to describe your InfluxDB metadata structure in a way that pyslet can understand. You
can decrease server startup time drastically by disabling this feature in your `.conf` file
(`[metadata] -> autogenerate=no`) after it has been generated once. You'll need to re-enable
it if your InfluxDB structure changes. You can also keep this feature disabled if you need
to hand-edit your .xml file to limit/change what is browseable to OData clients.

It is recommended that you run InfluxDB with auth enabled. Odata-influxdb passes through
http basic auth credentials to your InfluxDB server. You can specify a user in your
.conf file dsn settings. Example: `[influxdb] dsn=influxdb://user:pass@localhost:8086`

The default setting `[influxdb] max_items_per_query=50` is set extremely conservatively.
It is recommended to increase this value to as high as 1000 depending on your testing of
response times.

## Tests:

Run unit tests with `python tests.py`

## OData layout:

Upon startup, the server pulls the metadata from your InfluxDB server
(database names, measurement names, field keys, and tag keys).

Each measurement is set up as an OData table. All field keys and tag keys
from the InfluxDB database are included in the table, but many values
may be null depending on your InfluxDB setup. You can use OData $select 
query options to limit which columns are returned.

## Filters

OData $filter spec is supported, but has some limitations.

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

* InfluxDB requires a WHERE clause on the time field when grouping by time.*

* The release version of pyslet had a [bug](https://github.com/swl10/pyslet/issues/71) 
(now fixed) where you could not use a field called "time" so use "timestamp" to refer 
to InfluxDB's "time" field.*

* When using aggregate functions without grouping by '*', only influxdb fields
will be populated in the result, not tags. It is recommended to use 
`influxdbgroupby=*` in your queries, as it is not very expensive and
allows flexibility in your OData client processing.

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

