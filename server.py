import argparse
import logging
import os
import sys
from ConfigParser import ConfigParser
from wsgiref.simple_server import make_server

import pyslet.odata2.metadata as edmx
from pyslet.odata2.server import ReadOnlyServer

from influxdbmeta import generate_metadata
from influxdbds import InfluxDBEntityContainer

cache_app = None  #: our Server instance


class FileExistsError(IOError):
    def __init__(self, path):
        self.__path = path

    def __str__(self):
        return 'file already exists: {}'.format(self.__path)


def run_cache_server(port):
    """Starts the web server running"""
    server = make_server('', port, cache_app)
    logging.info("Starting HTTP server on port %i..." % port)
    # Respond to requests until process is killed
    server.serve_forever()


def load_metadata(metadata_filename, dsn):
    """Loads the metadata file and connects the InfluxDBEntityContainer."""
    doc = edmx.Document()
    with open(metadata_filename, 'rb') as f:
        doc.ReadFromStream(f)
    container = doc.root.DataServices['InfluxDBSchema.InfluxDB']
    InfluxDBEntityContainer(container=container, dsn=dsn)
    return doc


def start_server(service_root, doc):
    """starts the odata api server
    :param doc: the odata object to serve
    :type doc: edmx.Document
    
    :param service_root: example 'http://localhost:8080' 
    :type service_root: str
    """
    port = int(service_root[service_root.rfind(':') + 1:])
    server = ReadOnlyServer(serviceRoot=service_root)
    server.SetModel(doc)
    global cache_app
    cache_app = server
    run_cache_server(port)


def make_sample_config():
    config = ConfigParser(allow_no_value=True)
    config.add_section('server')
    config.set('server', 'server_root', 'http://localhost:8080')
    config.add_section('metadata')
    config.set('metadata', '; set autogenerate to "no" for quicker startup of the server if you know your influxdb structure has not changed')
    config.set('metadata', 'autogenerate', 'yes')
    config.set('metadata', '; metadata_file specifies the location of the metadata file to generate')
    config.set('metadata', 'metadata_file', 'generated.xml')
    config.add_section('influxdb')
    config.set('influxdb', 'dsn', 'influxdb://user:pass@localhost:8086')
    sample_name = 'sample.conf'
    if os.path.exists(sample_name):
        raise FileExistsError(sample_name)
    with open(sample_name, 'w') as cf:
        config.write(cf)
    print('generated sample conf at: {}'.format(os.path.join(os.getcwd(), sample_name)))


def main():
    """read config and start odata api server"""
    # parse arguments
    p = argparse.ArgumentParser()
    p.add_argument('-c', '--config', help='specify a conf file (default=production.conf)', default='production.conf')
    p.add_argument('-m', '--makeSampleConfig', help='generates sample.conf in your current directory (does not start server)',
                   action="store_true")
    args = p.parse_args()

    if args.makeSampleConfig:
        make_sample_config()
        sys.exit()

    # parse config file
    with open(args.config, 'r') as fp:
        c = ConfigParser()
        c.readfp(fp)
        metadata_filename = c.get('metadata', 'metadata_file')
        dsn = c.get('influxdb', 'dsn')

    # generate and load metadata
    if c.getboolean('metadata', 'autogenerate'):
        metadata = generate_metadata(dsn)
        with open(metadata_filename, 'wb') as f:
            f.write(metadata)
    doc = load_metadata(metadata_filename, dsn)
    start_server(c.get('server', 'server_root'), doc)



if __name__ == '__main__':
    main()
