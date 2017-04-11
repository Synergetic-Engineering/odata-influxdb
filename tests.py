import unittest
from server import load_metadata, make_server
from pyslet.odata2.client import Client, ClientCollection, ClientException
from pyslet.odata2.server import ReadOnlyServer
from pyslet.odata2 import csdl as edm
from pyslet.odata2 import core
import threading


SERVICE_PORT = 8080
SERVICE_ROOT = "http://localhost:%i/" % SERVICE_PORT


class TestInfluxOData(unittest.TestCase):
    def setUp(self):
        self._doc = load_metadata()
        self._container = self._doc.root.DataServices['InfluxDBSchema.InfluxDB']

    def testDatabases(self):
        container = self._container
        collection = container['DatabaseSet'].OpenCollection()
        self.assertGreater(len(collection), 0)
        for db in collection:
            self.assertIsInstance(db, unicode)
        self.assertIn(u'_internal', list(collection))
        internal = collection['_internal']
        self.assertEqual(internal['Name'], '_internal')

    def testKckThermoDatabase(self):
        container = self._container
        collection = container['DatabaseSet'].OpenCollection()
        self.assertGreater(len(collection), 0)
        for db in collection:
            self.assertIsInstance(db, unicode)
        self.assertIn(u'kck_thermo', list(collection))
        thermo = collection['kck_thermo']
        self.assertEqual(thermo['Name'], 'kck_thermo')
        with thermo['Measurements'].OpenCollection() as measurements:
            t = measurements['thermo']
            self.assertIsNotNone(t)
            with t['Points'].OpenCollection() as points:
                p_list = list(points.itervalues())
                self.assertIsInstance(p_list[0]['heatTransfer_use'].value, float)
                print list(p_list[0].iteritems())

    def testMeasurements(self):
        container = self._container
        collection = container['MeasurementSet'].OpenCollection()
        self.assertGreaterEqual(len(collection), 0)
        for measurement in collection:
            print measurement

    def testDatabaseMeasurement(self):
        container = self._container
        with container['DatabaseSet'].OpenCollection() as dbs:
            for db in dbs.itervalues():
                self.assertIsInstance(db, edm.Entity)
                m = db['Measurements']
                self.assertTrue(db['Measurements'].isCollection)
                with db['Measurements'].open() as measurements:
                    for m in measurements:
                        self.assertIsInstance(m, unicode)
                    for m in measurements.itervalues():
                        self.assertRaises(TypeError, lambda: m['Database']['Name'].value)
                        m_db = m['Database'].get_entity()
                        self.assertIsNotNone(m_db)
                        self.assertEqual(m_db['Name'].value, db['Name'].value)


class TestClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        doc = load_metadata()
        server = ReadOnlyServer(serviceRoot=SERVICE_ROOT)
        server.SetModel(doc)
        # The server is now ready to serve forever
        global cache_app
        cache_app = server
        """Starts the web server running"""
        cls._server = make_server('', SERVICE_PORT, cache_app)
        t = threading.Thread(target=cls._server.serve_forever)
        t.start()

    @classmethod
    def tearDownClass(cls):
        cls._server.shutdown()
        cls._server.socket.close()

    def testDatabaseEndpoint(self):
        c = Client('http://localhost:8080')
        f = c.feeds['DatabaseSet'].open()
        self.assertGreater(len(f), 0)
        for db in f.itervalues():
            self.assertIsInstance(db['Name'].value, unicode)
        internal = f['_internal']
        self.assertIsInstance(internal, core.Entity)
        measurements = internal['Measurements']
        self.assertTrue(measurements.isCollection)
        with measurements.open() as m:
            cq = m['cq']
        #self.assertGreater(len(f), 0)
        #for m in measurements:
        #    print m


if __name__ == '__main__':
    unittest.main()