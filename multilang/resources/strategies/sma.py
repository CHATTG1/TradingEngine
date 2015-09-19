def sma(data, window):
  if len(data) < window: return None
  return sum(data[-window:]) / float(window)
