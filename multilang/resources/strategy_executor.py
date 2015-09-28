#!/usr/bin/python
import storm, sys, time, os, imp

class PyBolt(storm.BasicBolt):
  def initialize(self, stormconf, context):
    # Load strategy definitions
    self.windows = {}
    self.strategies = {}
    load_strategies()

  def load_strategies():
    #TODO: Test with HDFS instead of local fs
    #os.call(['hadoop', 'fs', '-get', 'strategies', '.'])
    for strategy in os.listdir('strategies'):
      if 
      self.strategies[strategy] = load_from_file(strategy)

  def process(self, tuple):
    if tuple.component == '__system' and tuple.stream == '__tick': load_strategies()
    else: # Handle newly parsed metric update
      name = tuple.vales[0]
      price = tuple.vales[1]
      timestamp = tuple.values[3]

  def info(self, type, message): sys.stderr.write(type + ': ' + message + '\n')

  def load_from_file(filepath):
    class_inst = None
    expected_class = 'Strategy'
    mod_name,file_ext = os.path.splitext(os.path.split(filepath)[-1])

    if file_ext.lower() == '.py': py_mod = imp.load_source(mod_name, filepath)
    elif file_ext.lower() == '.pyc': py_mod = imp.load_compiled(mod_name, filepath)
    if hasattr(py_mod, expected_class): class_inst = getattr(py_mod, expected_class)()

    return class_inst

PyBolt().run()
