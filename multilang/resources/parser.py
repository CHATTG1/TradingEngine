#!/usr/bin/python

import storm, sys, json

class PyBolt(storm.BasicBolt):
  def initialize(self, stormconf, context): pass

#Uncomment for local testing
#for message in sys.stdin:
  def process(self, tuple):
    tup = json.loads(tuple.values[0])['resource']['fields']
    out = [tup['name'], float(tup['price']), tup['symbol'], int(tup['ts']), tup['type'], int(tup['volume'])]
    storm.emit(out)
    #Uncomment for local testing
    #print ', '.join(out)

# Uncomment for debug logging
#  def info(self,name, message): sys.stderr.write(name + ': ' + message)

PyBolt().run()
