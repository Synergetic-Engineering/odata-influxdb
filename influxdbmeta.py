from itertools import chain

from influxdb import InfluxDBClient

xml_head = """<?xml version="1.0" encoding="utf-8" standalone="yes" ?>
<edmx:Edmx Version="1.0" xmlns:edmx="http://schemas.microsoft.com/ado/2007/06/edmx"
           xmlns:m="http://schemas.microsoft.com/ado/2007/08/dataservices/metadata"
           xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xsi:schemaLocation="http://schemas.microsoft.com/ado/2007/06/edmx ">
    <edmx:DataServices m:DataServiceVersion="2.0">
        <Schema Namespace="InfluxDBSchema" xmlns="http://schemas.microsoft.com/ado/2006/04/edm"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="http://schemas.microsoft.com/ado/2006/04/edm ">"""

xml_foot = """
        </Schema>
    </edmx:DataServices>
</edmx:Edmx>"""

influx_type_to_edm_type = {
    'float': 'Edm.Double',  # influxdb stores floats in a float64 format
    'integer': 'Edm.Int64',  # influxdb stores integers as 64-bit signed
    'string': 'Edm.String'
}


def get_edm_type(influx_type):
    if influx_type is None:
        return 'Edm.String'
    else:
        return influx_type_to_edm_type[influx_type]


def mangle_measurement_name(m_name):
    """corresponds to unmangle_measurement_name in influxdbds.py"""
    m_name = m_name.replace(' ', '__sp__')
    return m_name


def mangle_db_name(db_name):
    """corresponds to unmangle_db_name in influxdbds.py"""
    return db_name.strip('_')  # edmx names cannot begin with '_'


def db_name__measurement_name(db_name, m_name):
    m_name = m_name.replace(' ', '__sp__')
    return '{}__{}'.format(
        mangle_db_name(db_name),
        mangle_measurement_name(m_name)
    )


class InfluxDB(object):
    def __init__(self, dsn):
        self.client = InfluxDBClient.from_DSN(dsn)

    def fields(self, db_name):
        """returns a tuple of dicts where each dict has attributes (name, type, edm_type)"""
        fields_rs = self.client.query('SHOW FIELD KEYS', database=db_name)
        tags_rs = self.client.query('SHOW TAG KEYS', database=db_name)
        # expand and deduplicate
        fields = set(tuple(f.items()) for f in chain(*chain(fields_rs, tags_rs)))
        fields = (dict(
            name=f[0][1],
            type='string' if len(f)==1 else f[1][1],
            edm_type=get_edm_type('string' if len(f)==1 else f[1][1])
        ) for f in fields)
        return tuple(fields)

    @property
    def measurements(self):
        measurements = []
        for db in self.databases:
            q = 'SHOW MEASUREMENTS'
            rs = self.client.query(q, database=db[u'name'])

            def m_dict(m):
                d = dict(m)
                d['db_name'] = db['name']
                d['mangled_db'] = mangle_db_name(db['name'])
                d['mangled_measurement'] = mangle_measurement_name(m['name'])
                d['mangled_path'] = db_name__measurement_name(db['name'], m['name'])
                d['fields'] = self.fields(db['name'])
                return d
            measurements.extend(m_dict(m) for m in rs.get_points())
        return measurements

    @property
    def databases(self):
        rs = self.client.get_list_database()
        return iter(rs)


def gen_entity_set_xml(m):
    return '<EntitySet Name="{}" EntityType="InfluxDBSchema.{}"/>'.format(m['mangled_path'], m['mangled_path'])


def generate_properties_xml(m):
    return '\n'.join(
        '<Property Name="{}" Type="{}" Nullable="true" />'.format(f['name'], f['edm_type']) for f in m['fields']
    )


def generate_key_xml(m):
    """influxdb has no concept of a key, so we use the time value (NOT gauranteed to be unique)"""
    return '<Key><PropertyRef Name="timestamp" /></Key><Property Name="timestamp" Type="Edm.DateTime" Precision="6" Nullable="false" />'


def gen_entity_type_xml(m):
    return '<EntityType Name="{}">{}\n{}</EntityType>'.format(
        m['mangled_path'],
        generate_key_xml(m),
        generate_properties_xml(m))


def entity_sets_and_types(db):
    """generate xml entries for entity sets (containers) and entity types (with properties)"""
    entity_sets = []
    entity_types = []
    for m in db.measurements:
        entity_sets.append(gen_entity_set_xml(m))
        entity_types.append(gen_entity_type_xml(m))
    return entity_sets, entity_types


def generate_metadata(dsn):
    """connect to influxdb, read the structure, and return an edmx xml file string"""
    i = InfluxDB(dsn)
    entity_sets, entity_types = entity_sets_and_types(i)
    output = """{}
    <EntityContainer Name="InfluxDB" m:IsDefaultEntityContainer="true">
    {}
    </EntityContainer>
    {}
    {}""".format(xml_head, '\n'.join(entity_sets), '\n'.join(entity_types), xml_foot)
    return output

