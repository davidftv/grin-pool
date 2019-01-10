CONTAINER := grinpool/stratum
REGISTRY := docker.io

build:
	cargo build --release
	cp target/release/grin-pool grin-pool
	docker build $(DOCKER_BUILD_FLAGS) -t $(CONTAINER) .
	docker tag $(CONTAINER):latest $(REGISTRY)/$(CONTAINER):latest
#	docker push $(REGISTRY)/$(CONTAINER):latest
