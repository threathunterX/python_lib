#!/bin/bash
DIR=/home/threathunter/nebula/nebula_web/venv/bin

build_help()
{
  echo "nebula cron inspect script"
  echo "Usage:"
  echo "      cron_inspect.py [options] [args]"
  echo "Options:"
  echo "--check_history [duration]: 检查时间范围内的数据统计的正确性"
  echo "ex. --check_history 1d 检查今天(当前小时之前)的统计是否运行没错"
  echo "    --check_history 1m 检查今天之前一个月的统计是否运行没错"
  echo "    --check_history 1w 检查今天之前一个星期的统计是否运行没错"
  echo "    --check_history 5h 检查之前5个小时的统计是否运行没错"
  echo "    --check_history 检查上个小时的统计"
  echo "--rerun_history [duration]: 检查时间范围内的数据统计的正确性,并重新统计有问题的目录, 如果客户流量较大或者检查时间范围较大，可能会耗时较久"
  echo "--rerun [db_path db_path1 ...] : 默认检查上个小时的统计，如果有错就重新跑； 也可以指定数据文件地址"
}

check_history()
{
  if [ ! -n "$1" ]; then
    echo "必须输入时间范围"
  else
    echo "时间范围为$1"
    $DIR/python $DIR/cron_inspect.py --check_history $1
  fi
  exit 0
}

rerun_history()
{
  unit=${1:((${#1}-1))}
  value=${1:0:((${#1}-1))}
  case $unit in
    h) unit_time=hours;;
    d) unit_time=days;;
    w) unit_time=weeks;;
    m) unit_time=mouths;;
    *) echo "invalid options $1"; exit 0;;
  esac
  cd /data/persistent
  MIN_LIMIT=`date -d "$value $unit_time ago" +%Y%m%d%H`
  MAX_LIMIT=`date -d "1 hours ago" +%Y%m%d%H`
  history_paths=`ls -d * | awk -v MIN_LIMIT=$MIN_LIMIT -v MAX_LIMIT=$MAX_LIMIT '$1>=MIN_LIMIT && $1<=MAX_LIMIT{print "/data/persistent/"$1}'`
  for path in ${history_paths[@]}
  do
    echo "开始重新执行$path"
    $DIR/python $DIR/cron_inspect.py --rerun $path
  done
}

rerun()
{
  if [ ! -n "$1" ]; then
    echo "必须输入日志路径"
  else
    while (($#>0))
    do
      echo "开始重新执行$1"
      $DIR/python $DIR/cron_inspect.py --rerun $1 
      shift
    done
  fi
  exit 0
}

while [ -n "$1" ]
do
  case $1 in
    -h|--help)  build_help; exit 0;;
    --check_history)  check_history $2; shift 2;;
    --rerun_history)  rerun_history $2; shift 2;;
    --rerun) shift; rerun $@;;
    *)  echo "invalid options $1"; build_help; exit;;
  esac
done
