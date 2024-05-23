python -m build
scp ./dist/*.whl root@192.168.100.5:/storage/pip-server/
ssh root@192.168.100.5 "cd ~/local-services; docker-compose restart pip-server"