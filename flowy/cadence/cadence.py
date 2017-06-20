from tornado import gen, ioloop
from tchannel import thrift

import os.path

cadence_frontend = thrift.load(
    path = os.path.join(os.path.dirname(__file__), 'idl/github.com/uber/cadence/cadence.thrift'),
    service='cadence-frontend',
    hostport='localhost:7933'
)
shared = cadence_frontend.shared
