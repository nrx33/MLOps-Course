LOCAL_TAG:=$(shell date +"%Y-%m-%d-%H-%M")
LOCAL_IMG_TAG="stream-model-duration:${LOCAL_TAG}"

quality_check:
	isort .
	black .
	pylint --recursive=y .

unit_test: 
	pytest -p no:warnings ./tests

build: quality_check unit_test
	docker build -t ${LOCAL_IMG_TAG} .

integration_test: build
	bash ./integration-test/run.sh 

run: integration_test
	echo "executing the commands"