#!/usr/bin/env bash

sed -i '1,/This container IP/!d' /usr/bin/init-script # remove the while loop at the end
/usr/bin/init-script
echo "[ $(date) ] System initialized, starting demo app..."

# LD_LIBRARY_PATH=/opt/mapr/lib nohup /root/.local/bin/uv run /app/main.py &
LD_LIBRARY_PATH=/opt/mapr/lib nohup .venv/bin/streamlit run main.py &

[ -f nohup.out ] && tail -f nohup.out # so docker logs will show logs

sleep infinity # just in case, keep container running
