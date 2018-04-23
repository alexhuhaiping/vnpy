#/bin/bash

cd /srv/vnpy/optization

python -O optserver.py &

prog_exit()
{
    ps -ef| grep "python -O optserver.py" |grep -v grep |awk '{print $2}'|xargs kill -15

}

trap "prog_exit" 15

flag=1
while [ $flag -ne 0 ];do
    sleep 1;
    flag=`ps -ef| grep "python -O optserver.py" |grep -v grep | wc -l`
done;

