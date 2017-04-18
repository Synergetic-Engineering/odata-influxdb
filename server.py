import pyslet.odata2.metadata as edmx
from pyslet.odata2.server import ReadOnlyServer
from influxdb_dal.influxdbds import InfluxDBEntityContainer
from generate_metadata import generate_metadata
from config import INFLUXDB_DSN

SERVICE_PORT = 8080
SERVICE_ROOT = "http://localhost:%i/" % SERVICE_PORT

import logging
from wsgiref.simple_server import make_server

cache_app = None  #: our Server instance


def run_cache_server():
    """Starts the web server running"""
    server = make_server('', SERVICE_PORT, cache_app)
    logging.info("Starting HTTP server on port %i..." % SERVICE_PORT)
    # Respond to requests until process is killed
    server.serve_forever()


def load_metadata():
    """Loads the metadata file from the current directory."""
    doc = edmx.Document()
    with open('generated.xml', 'rb') as f:
        doc.ReadFromStream(f)
    container = doc.root.DataServices['InfluxDBSchema.InfluxDB']
    InfluxDBEntityContainer(container=container, dsn=INFLUXDB_DSN)
    return doc


def main():
    """Executed when we are launched"""
    with open('generated.xml', 'wb') as f:
        f.write(generate_metadata(INFLUXDB_DSN))
    doc = load_metadata()
    server = ReadOnlyServer(serviceRoot=SERVICE_ROOT)
    server.SetModel(doc)
    # The server is now ready to serve forever
    global cache_app
    cache_app = server
    run_cache_server()


if __name__ == '__main__':
    main()
