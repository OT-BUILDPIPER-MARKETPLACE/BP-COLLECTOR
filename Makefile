VERSION ?= 1.0
CONF_PATH ?=${PWD}/config/export_aws_resources_snapshots_sample.yml

build:
	docker build -t opstree/collector:${VERSION} .
run:
	docker run -it --rm --name collector -v ${CONF_PATH}:/opt/config/export_aws_resources_snapshots_sample.yml:ro -e SCHEDULE_ACTION=${ACTION} -e CONF_PATH='/opt/config/export_aws_resources_snapshots_sample.yml' -v ~/.aws:/root/.aws opstree/collector:${VERSION} 
