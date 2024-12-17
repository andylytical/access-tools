DIRNAME := $(shell basename $$(pwd))
HOME := $(shell echo $$HOME)

go:
	bash go.sh

test:
	echo $(DIRNAME)
	echo $(HOME)

build:
	docker build -t $(DIRNAME) src

run:
	docker run -it --rm --mount type=bind,src=$(HOME),dst=/home $(DIRNAME)

clean:
	docker container prune -f
	docker images | awk '/access-tools/ {print $$3}' | xargs -r docker rmi
	docker system prune -f
