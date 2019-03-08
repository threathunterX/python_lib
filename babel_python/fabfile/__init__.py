#coding:utf-8

import os

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))

from fabric.state import env

from essay.tasks import build
from essay.tasks import deploy
#from essay.tasks import nginx
#from essay.tasks import supervisor

env.GIT_SERVER = 'git.coding.net'  # ssh地址只需要填：github.com
env.PROJECT = 'babel_python'
env.BUILD_PATH = '/alidata1/threathunter/build/'
env.PROJECT_OWNER = 'inject2006'
#env.DEFAULT_BRANCH = 'dev'
env.PYPI_INDEX = 'http://192.168.1.3:3141/threathunter/dev/+simple/'


######
# deploy settings:

env.PROCESS_COUNT = 2  #部署时启动的进程数目
env.password = 'threathunter1qazQAZ'
env.roledefs = {
    'build': ['threathunter@192.168.1.3:22'],  # 打包服务器配置
    'dev': ['threathunter@192.168.1.7:22'],
    'release': ['threathunter@192.168.1.2:22'],
}

env.VIRTUALENV_PREFIX = '/alidata1/threathunter/projects/'
env.SUPERVISOR_CONF_TEMPLATE = os.path.join(PROJECT_ROOT, 'conf', 'supervisord.conf')
