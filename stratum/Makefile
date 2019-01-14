CONTAINER := stratumx
REGISTRY := docker.mwgrinpool.com:5001

build:
	OPENSSL_STATIC=yes cargo build --release
	cp target/release/grin-pool grin-pool
	#docker build $(DOCKER_BUILD_FLAGS) -t $(CONTAINER) .
	docker build --network=host $(DOCKER_BUILD_FLAGS) -t $(CONTAINER) .
	docker tag $(CONTAINER):latest $(REGISTRY)/$(CONTAINER):latest
#	docker push $(REGISTRY)/$(CONTAINER):latest
