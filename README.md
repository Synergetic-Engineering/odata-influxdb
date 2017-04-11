# influxdb_odata

This project allows you to use the [OData REST API](http://www.odata.org/) to access [InfluxDB](https://www.influxdata.com/) data.

## Requirements:


python: Currently requires Python 2.

[pyslet](https://github.com/swl10/pyslet): The OData functionality from the pyslet project is used in this project.

## Usage:

Create a config file at `influxdb_dal/config.py`. Minimal example shown below:

```python
#config.py
INFLUXDB_DSN = 'influxdb://<user>:<pass>@fqdn'
```

Start your odata endpoint server with `python server.py`.

Point an OData browser to `http://hostname:8080/DatabaseSet('kck_thermo')/Measurements('thermo')/Points`

## Tests:

Run unit tests with `python tests.py`

## Notes:

Currently the unit tests and working code are dependant on a specific server and metadata structure setup (kck_thermo).
