### CONFIG
export MSYS_NO_PATHCONV=1; # git-bash workaroung for Windows

my_dir="$(dirname "$0")";

### PROGRAM
docker-compose up -d;
echo 'Waiting for docker services...';
while ! docker exec mysql-events_mysql55_1 mysqladmin ping -h'127.0.0.1' --silent; do
  sleep 3
done
while ! docker exec mysql-events_mysql56_1 mysqladmin ping -h'127.0.0.1' --silent; do
  sleep 3
done
while ! docker exec mysql-events_mysql80_1 mysqladmin ping -h'127.0.0.1' --silent; do
  sleep 3
done

docker run --rm -t \
  --net=host \
  -v `pwd`:/app \
  -w /app node:8-alpine \
  /bin/sh -c "npm install && npm test"

exitCode=$?;

docker-compose down -v;

exit $exitCode;
