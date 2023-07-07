date
cd "/root/work/datafeeder"
/usr/local/bin/pm2 start "symboladder.py" --interpreter="/usr/bin/python3" --name="symboladder" --restart-delay=90000 --time 
sleep 20s
/usr/local/bin/pm2 start "tickdata.py" --interpreter="/usr/bin/python3" --name="tickdata" --restart-delay=90000 --time 
sleep 5s
/usr/local/bin/pm2 start "candle.py" --interpreter="/usr/bin/python3" --name="candle" --restart-delay=90000 --time 



cd "/root/work/tradeExecutor"
/usr/local/bin/pm2 start "ordersender.py" --interpreter="/usr/bin/python3" --name="ordersender" --restart-delay=90000 --time
sleep 5s

cd "/root/work/modifyorder"
/usr/local/bin/pm2 start "modifyorder.py" --interpreter="/usr/bin/python3" --name="modifyorder" --restart-delay=90000 --time
sleep 5s

cd "/root/work/orderResponse"
/usr/local/bin/pm2 start "responsehandling.py" --interpreter="/usr/bin/python3" --name="responsehandling" --restart-delay=90000 --time

cd "/root/work/orderResponse"
/usr/local/bin/pm2 start "missingresponse.py" --interpreter="/usr/bin/python3" --name="missingresponse" --restart-delay=90000 --time






sleep 5s
cd "/root/work/HopperN"
/usr/local/bin/pm2 start "strategyLauncher.py" --interpreter="/usr/bin/python3" --name="HopperN" --restart-delay=90000 --time

# /usr/local/bin/pm2 start clientLauncher.py --name MagicRSTEST_Exe  --interpreter=/root/OrderHandler/.venv/bin/python3 --restart-delay=90000 --time -- MagicRSTEST
                   