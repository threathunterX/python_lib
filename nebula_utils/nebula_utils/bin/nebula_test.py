#!/home/threathunter/nebula/nebula_web/venv/bin/python

from os import path

from nebula_utils.persist_compute.main import compute_statistic
from nebula_utils.persist_compute import cache
from nebula_utils import settings


to_compute_hours = ['18', ]
to_compute_dates = ['20160810', ]
for date in to_compute_dates:
    for h in to_compute_hours:
        datetime = date + h
        db_path = path.join(settings.DB_ROOT_PATH, datetime)
        compute_statistic(db_path)
        cache.Stat_Dict = dict()
        cache.Hook_Functions = []
