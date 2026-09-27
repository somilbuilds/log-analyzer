deb http://archive.debian.org/debian stretch main
deb http://archive.debian.org/debian-security stretch/updates main
apt-get -o Acquire::Check-Valid-Until=false update
apt-get install -y python3
