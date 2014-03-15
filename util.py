"""
Contains utility functions used in other files.
"""

from datetime import datetime
from random import randint


def to_base36(value):
  """
  Converts an int to base 36 string. Only works for positive numbers.
  """
  value = int(value)

  if value == 0:
      return '0'

  result = []
  while value:
      value, mod = divmod(value, 36)
      result.append('0123456789abcdefghijklmnopqrstuvwxyz'[mod])

  return ''.join(reversed(result))

def js_getTime():
  """
  Gets the number of milliseconds since 1970/01/01.
  Does exactly what js getTime does.
  """
  dt = datetime.now() - datetime(1970,1,1)
  ms = (dt.days * 24 * 60 * 60 + dt.seconds) * 1000
  return ms

def get_aid():
    """Returns the aid to submit a post."""
    return to_base36(js_getTime()) + to_base36(randint(0,1679616))