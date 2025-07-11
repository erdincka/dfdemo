#!/usr/bin/env bash

sed -i '1,/This container IP/!d' /usr/bin/init-script # remove the while loop at the end
/usr/bin/init-script
echo "[ $(date) ] System initialized, starting demo app..."

LD_LIBRARY_PATH=/opt/mapr/lib nohup /app/.venv/bin/streamlit run /app/main.py &

[ -f nohup.out ] && tail -f nohup.out # so docker logs will show logs
#
git config --global credentials.helper store
#
sleep infinity # just in case, keep container running
