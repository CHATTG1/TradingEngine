#!/usr/bin/python
import storm, sys, time, os

class PyBolt(storm.BasicBolt):
  def initialize(self, stormconf, context):
    # Load strategy definitions
    self.windows = {}
    self.strategies = {}
    load_strategies()

  def load_strategies():
    #os.call(['hadoop', 'fs', '-get', 'strategies', '.'])
    for strategy in os.listdir('strategies'):
      self.strategies[strategy] = 
      import strategy


  def process(self, tuple):
    # If it's a tick tuple, emit current metric counts
    if tuple.component == '__system' and tuple.stream == '__tick': pass
      load_strategies()

    else: # Handle newly parsed metric update
      name = tuple.vales[0]
      price = tuple.vales[1]
      timestamp = tuple.values[3]

      if severity != '':
        metric_key = '|'.join([host, facility, severity])
        if metric_key in self.metrics: self.metrics[metric_key] += 1
        else: self.metrics[metric_key] = 1

  def info(self, type, message): sys.stderr.write(type + ': ' + message + '\n')

PyBolt().run()
