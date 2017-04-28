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
* lt (less than, <)
* eq (equals, =)
* ne (not equal to, !=)
