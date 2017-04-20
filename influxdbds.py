import datetime
import numbers

import influxdb
from functools32 import lru_cache
from pyslet.odata2.core import EntityCollection, NavigationCollection, Entity


@lru_cache()
def get_tags_and_field_keys(client, measurement_name, db_name):
    client.switch_database(db_name)
    field_keys = tuple(f['fieldKey'] for f in client.query('show field keys')[measurement_name])
    tag_keys = tuple(t['tagKey'] for t in client.query('show tag keys')[measurement_name])
    return tuple(field_keys + tag_keys)


class InfluxDBEntityContainer(object):
    """Object used to represent an Entity Container (influxdb database)

    modelled after the SQLEntityContainer in pyslet (sqlds.py)

    container
        pyslet.odata2.csdl.EntityContainer

    dsn
        data source name in the format: influxdb://user:pass@host:port/
        supported schemes include https+influxdb:// and udp+influxdb://
    """
    def __init__(self, container, dsn, **kwargs):
        self.container = container
        self.dsn = dsn
        self.client = influxdb.InfluxDBClient.from_DSN(self.dsn)
        for es in self.container.EntitySet:
            self.bind_entity_set(es)

    def bind_entity_set(self, entity_set):
        entity_set.bind(self.get_collection_class(), container=self)

    def get_collection_class(self):
        return InfluxDBMeasurement


class InfluxDBMeasurement(EntityCollection):
    """represents a measurement query, containing points

    name should be "database.measurement"
    """
    def __init__(self, container, **kwargs):
        super(InfluxDBMeasurement, self).__init__(**kwargs)
        self.container = container
        self.db_name, self.measurement_name = self.entity_set.name.split('__')
        self.topmax = 100

    def __len__(self):
        """influxdb only counts non-null values, so we return the count of the field with maximum non-null values"""
        q = 'SELECT COUNT(*) FROM %s' % self.measurement_name
        self.container.client.switch_database(self.db_name)
        rs = self.container.client.query(q)
        max_count = max(val for val in rs.get_points().next().values() if isinstance(val, numbers.Number))
        self._influxdb_len = max_count
        return max_count

    def itervalues(self):
        return self.order_entities(
            self.expand_entities(self.filter_entities(
                self.generate_entities())))

    def generate_entities(self):
        q = "SELECT * FROM %s" % self.measurement_name
        result = self.container.client.query(q, database=self.db_name)
        fields = get_tags_and_field_keys(self.container.client, self.measurement_name, self.db_name)
        for m in result[self.measurement_name]:
            t = datetime.datetime.strptime(m['time'], '%Y-%m-%dT%H:%M:%SZ')
            e = self.new_entity()
            e['time'].set_from_value(t)
            for f in fields:
                e[f].set_from_value(m[f])
            e.exists = True
            yield e

    def __getitem__(self, key):
        raise NotImplementedError

    def set_page(self, top, skip=0, skiptoken=None):
        self.top = top or self.topmax or 10 # a None value for top causes the default iterpage method to set a skiptoken
        self.skip = skip
        self.skiptoken = skiptoken
        self.nextSkiptoken = None

