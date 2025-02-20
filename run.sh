python3 server1.py -cluster 1 -port 8001 &

sleep 2

python3 server2.py -cluster 1 -port 8002 &

sleep 2

python3 server3.py -cluster 1
