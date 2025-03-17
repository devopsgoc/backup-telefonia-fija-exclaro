tagname="devopsgoc/priv:backup-telefonia-fija-exclaro_0.0.1"
sudo docker build -t "${tagname}" .
sleep 1
sudo docker push "${tagname}"