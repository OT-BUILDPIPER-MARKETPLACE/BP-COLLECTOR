build:
	docker build -t opstree/redis_backup:0.1 .

run:
	docker run -it --rm --name backup_redis -v ${PWD}/config/redis_backup.yml:/opt/config/redis_backup.yml:ro  -e CONF_PATH='/opt/config/redis_backup.yml' -v ~/.aws:/root/.aws opstree/redis_backup:0.1 

run-debug:
	docker run -it --rm --name  backup_redis -v ${PWD}/config/redis_backup.yml:/opt/config/redis_backup.yml:ro -e CONF_PATH='/opt/config/redis_backup.yml' -v ~/.aws:/root/.aws --entrypoint bash backup_redis
