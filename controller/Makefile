IMAGE ?= romadomor/smart-scheduler:v0.2
PLATFORM ?= linux/amd64
LATEST_IMAGE := $(firstword $(subst :, ,$(IMAGE))):latest

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

.PHONY: deploy
.PHONY: deploy
.PHONY: deploy
deploy:
	kubectl apply -f manifests/rbac.yaml
	kubectl apply -f manifests/configmap.yaml
	kubectl apply -f manifests/service.yaml
	kubectl apply -f manifests/webhook-service.yaml
	kubectl apply -f manifests/deployment.yaml
	./scripts/gen-certs.sh
	kubectl rollout status deploy/smart-scheduler-controller -n kube-system

.PHONY: logs
logs:
	kubectl logs -n kube-system -l app=smart-scheduler-controller -f