import os
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import pandas as pd
import numpy as np
import apache_beam as beam
import re
import warnings
warnings.filterwarnings("ignore")
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import time
import json