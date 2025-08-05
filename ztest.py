import pandas as pd
import os
from src.python.classes.CallsClass import CallsClass


callings = CallsClass()
df = callings.load_and_parse_forms()

