# GRIN
CONTAINER := grin
REGISTRY := docker.io/grinpool

default: build push

build:
#	curl -s https://raw.githubusercontent.com/mimblewimble/grin/master/Dockerfile > Dockerfile
#	sed -i 's/^COPY \. \..*$$/RUN git clone https:\/\/github.com\/mimblewimble\/grin.git .; git checkout master; git pull;/' Dockerfile
#	sed -i 's/ENTRYPOINT.*$$/CMD ["\/run.sh"]/' Dockerfile
#	echo 'COPY run.sh /' >> Dockerfile
	cargo build --release
	cp target/release/grin grin
	docker build $(DOCKER_BUILD_FLAGS) -t $(CONTAINER)  .
	docker tag $(CONTAINER):latest $(REGISTRY)/$(CONTAINER):latest

push:
	docker push $(REGISTRY)/$(CONTAINER):latest
