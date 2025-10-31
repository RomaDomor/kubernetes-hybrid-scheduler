IMAGE_REPO ?= romadomor/smart-scheduler
VERSION ?= v0.3
IMAGE := $(IMAGE_REPO):$(VERSION)

PLATFORM ?= linux/amd64
LATEST_IMAGE := $(IMAGE_REPO):latest

.PHONY: buildx-setup
buildx-setup:
	docker buildx create --use --name multi 2>/dev/null || true

.PHONY: build
build: buildx-setup
	docker buildx build \
		--platform $(PLATFORM) \
		-t $(IMAGE) \
		--load .

.PHONY: push
push: buildx-setup
	docker buildx build \
		--platform $(PLATFORM) \
		-t $(IMAGE) \
		-t $(LATEST_IMAGE) \
		--push .

.PHONY: logs
logs:
	kubectl logs -n kube-system -l app=smart-scheduler-controller -f