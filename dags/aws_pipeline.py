import itertools
import json
import os
import re
from datetime import timedelta, datetime

from airflow.sdk import dag, task, Param, get_current_context
import logging

logger = logging.getLogger(__name__)

class Config:
     pass