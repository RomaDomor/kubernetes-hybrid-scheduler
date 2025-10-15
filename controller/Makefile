IMAGE = romadomor/smart-scheduler:v0.1

.PHONY: build
build:
	docker build -t $(IMAGE) .

.PHONY: push
push: build
	docker push $(IMAGE)

.PHONY: deploy
deploy:
	kubectl apply -f manifests/rbac.yaml
	kubectl apply -f manifests/configmap.yaml
	kubectl apply -f manifests/service.yaml
	kubectl apply -f manifests/deployment.yaml

.PHONY: logs
logs:
	kubectl logs -n kube-system -l app=smart-scheduler-controller -f