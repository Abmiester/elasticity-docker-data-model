model_name = elasticity-docker-data-model
.PHONY: install
install:
	sudo cp new.sh /usr/local/bin/newmodel
	sudo chmod +x /usr/local/bin/newmodel

.PHONY: build
build:
	docker build -t $(model_name) .

.PHONY: build_clean
build_clean:
	docker build -t $(model_name) --no-cache .

.PHONY: run
run: build
	docker run --env-file=.env $(model_name) python3 run.py