import datetime

from functools32 import lru_cache
from pyslet.odata2.core import EntityCollection, NavigationCollection, Entity
from influxdb import InfluxDBClient

from config import INFLUXDB_DSN

client = InfluxDBClient.from_DSN(INFLUXDB_DSN)


@lru_cache()
def get_tags_and_field_keys(measurement_name, db_name=None):
    if db_name is not None:
        client.switch_database(db_name)
    field_keys = tuple(f['fieldKey'] for f in client.query('show field keys')[measurement_name])
    tag_keys = tuple(t['tagKey'] for t in client.query('show tag keys')[measurement_name])
    return tuple(field_keys + tag_keys)


class InfluxDatabaseCollection(EntityCollection):
    def __init__(self, *args, **kwargs):
        super(InfluxDatabaseCollection, self).__init__(*args, **kwargs)

    def itervalues(self):
        return self.order_entities(
            self.expand_entities(self.filter_entities(
                self.generate_entities())))

    def generate_entities(self):
        """list all databases"""
        result = client.get_list_database()
        for db in result:
            e = self.new_entity()
            e['Name'].set_from_value(db[u'name'])
            e.exists = True
            yield e

    def __getitem__(self, item):
        db_list = client.get_list_database()
        if item in (db['name'] for db in db_list):
            e = self.new_entity()
            e['Name'].set_from_value(item)
            e.exists = True
            if self.check_filter(e):
                if self.expand or self.select:
                    e.expand(self.expand, self.select)
                return e
        else:
            raise KeyError('database does not exist: %s' % item)


class DatabaseMeasurements(NavigationCollection):
    def __init__(self, *args, **kwargs):
        self.from_entity = kwargs['from_entity']
        super(NavigationCollection, self).__init__(*args, **kwargs)

    def itervalues(self):
        return self.order_entities(
            self.expand_entities(self.filter_entities(
                self.generate_entities()
            ))
        )

    def generate_entities(self):
        db_name = self.from_entity['Name'].value
        client.switch_database(db_name)
        result = client.query("SHOW MEASUREMENTS")
        for m in result['measurements']:
            e = self.new_entity()
            e['Name'].set_from_value(m['name'])
            e['DatabaseName'].set_from_value(self.from_entity['Name'])
            e.exists = True
            yield e

    def __getitem__(self, item):
        db_name = self.from_entity['Name'].value
        client.switch_database(db_name)
        result = client.query("SHOW MEASUREMENTS")
        m = next((m for m in result['measurements'] if m['name'] == item), None)
        if m is not None:
            e = self.new_entity()
            e['Name'].set_from_value(item)
            e.exists = True
            return e
        else:
            raise KeyError('database %s does not contain measurement: %s' % (db_name, item))


class MeasurementDatabase(NavigationCollection):
    def itervalues(self):
        return self.order_entities(
            self.expand_entities(self.filter_entities(
                self.generate_entities()
            ))
        )

    def generate_entities(self):
        db_name = self.from_entity['DatabaseName'].value
        e = self.new_entity()
        e['Name'].set_from_value(db_name)
        e.exists = True
        yield e



class MeasurementPoints(NavigationCollection):
    def __init__(self, *args, **kwargs):
        self.from_entity = kwargs['from_entity']

        super(NavigationCollection, self).__init__(*args, **kwargs)

    def itervalues(self):
        return self.order_entities(
            self.expand_entities(self.filter_entities(
                self.generate_entities()
            ))
        )

    def generate_entities(self):
        measurement_name = self.from_entity['Name'].value
        db_name = 'kck_thermo' #Todo: self.from_entity['Database']['Name'].value
        client.switch_database(db_name)
        result = client.query("SELECT * FROM %s" % measurement_name)
        fields = get_tags_and_field_keys(measurement_name)
        for m in result[measurement_name]:
            e = self.new_entity()
            #format: 2016-10-22T13:50:00Z
            t = datetime.datetime.strptime(m['time'], '%Y-%m-%dT%H:%M:%SZ')
            e['time'].set_from_value(t)
            for f in fields:
                e[f].set_from_value(m[f])
            e.exists = True
            yield e

    def __getitem__(self, item):
        print(item)
        measurement_name = self.from_entity['Name'].value
        db_name = 'kck_thermo' #TODO: self.from_entity['Database']['Name'].value
        client.switch_database(db_name)
        q = "SELECT * FROM %s WHERE time='%sZ'" % (measurement_name, item['time'])
        print(q)
        result = client.query(q)
        fields = get_tags_and_field_keys(measurement_name)
        if result is not None:
            m = result[measurement_name].next()
            e = self.new_entity()
            #format: 2016-10-22T13:50:00Z
            t = datetime.datetime.strptime(m['time'], '%Y-%m-%dT%H:%M:%SZ')
            e['time'].set_from_value(t)
            for f in fields:
                e[f].set_from_value(m[f])
            e.exists = True
            return e
        else:
            raise KeyError('database %s does not contain measurement: %s' % (db_name, item))
