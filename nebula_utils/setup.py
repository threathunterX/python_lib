# -*- coding: utf-8 -*-

from distutils.core import setup

#from nebula_utils.persist_utils.settings import Nebula_Utils_Version

setup(name='nebula_utils',
      version='1.1.1',
      description='nebula_utils is nebula common utils func set',
      author='nebula',
      author_email='nebula@threathunter.cn',
      url='http://www.threathunter.cn',
      packages=['nebula_utils', 'nebula_utils.persist_utils', 'nebula_utils.persist_compute', 'nebula_utils.persist_utils.bson'],
      scripts=['nebula_utils/bin/nebula_clean.py', 'nebula_utils/bin/nebula_compute.py',
               'nebula_utils/bin/nebula_test.py', 'nebula_utils/bin/offline_stat_server.py',
               'nebula_utils/bin/cron_inspect.py', 'nebula_utils/bin/cron_inspect.sh'],
      data_files=[
#          ('/etc/cron.d/',['nebula_utils/cron/nebula_compute', ]),
#          ('/usr/bin/', ['nebula_utils/bin/nebula_clean.py', 'nebula_utils/bin/nebula_compute.py', 'nebula_utils/bin/nebula_test.py']),
          # data_files的执行是在scripts之前, shebang里面并没有如同scripts里面改变..所以这个cronjob不能通过python来安装了...
      ],
)
