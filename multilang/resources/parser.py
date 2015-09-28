#!/usr/bin/python

import storm, sys, json

class PyBolt(storm.BasicBolt):
  def initialize(self, stormconf, context): pass

#Uncomment for local testing
#for message in sys.stdin:
  def process(self, tuple):
    tup = json.loads(tuple.values[0])['resource']['fields']
    out = [tup['name'], tup['price'], tup['symbol'], tup['ts'], tup['type'], tup['volume']]
    storm.emit(out)
    #Uncomment for local testing
    #print ', '.join(out)

# Uncomment for debug logging
#  def info(self,name, message): sys.stderr.write(name + ': ' + message)

PyBolt().run()
