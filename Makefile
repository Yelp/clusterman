PKG_NAME=clusterman
DOCKER_TAG ?= ${PKG_NAME}-dev-$(USER)
VIRTUALENV_RUN_TARGET = virtualenv_run-dev
VIRTUALENV_RUN_REQUIREMENTS = requirements.txt requirements-dev.txt

.PHONY: all
all: development

# https://www.gnu.org/software/make/manual/html_node/Target_002dspecific
.PHONY: production
production: virtualenv_run
production: export VIRTUALENV_RUN_REQUIREMENTS = requirements.txt
production: export VIRTUALENV_RUN_TARGET = virtualenv_run

.PHONY: development
development: virtualenv_run install-hooks

.PHONY: docs
docs:
	-rm -rf docs/build
	tox -e docs

.PHONY: test
test: clean-cache
	tox -e yelp

.PHONY: test-external
test-external: clean-cache
	tox -e external -- --tags=-yelp

.PHONY: itest
itest: export EXTRA_VOLUME_MOUNTS=/nail/etc/services/services.yaml:/nail/etc/services/services.yaml:ro
itest: cook-image
	tox -e acceptance
	./service-itest-runner clusterman.batch.spot_price_collector "--aws-region=us-west-1 "
	./service-itest-runner clusterman.batch.cluster_metrics_collector "--cluster=docker"
	./service-itest-runner clusterman.batch.autoscaler_bootstrap "" clusterman.batch.autoscaler

.PHONY: itest-external
itest-external: cook-image-external
	tox -e acceptance
	./service-itest-runner examples.batch.spot_price_collector "--aws-region=us-west-1 --env-config-path=acceptance/srv-configs/clusterman-external.yaml"
	./service-itest-runner examples.batch.cluster_metrics_collector "--cluster=docker --env-config-path=acceptance/srv-configs/clusterman-external.yaml"
	./service-itest-runner examples.batch.autoscaler_bootstrap "--env-config-path=acceptance/srv-configs/clusterman-external.yaml" examples.batch.autoscaler

.PHONY: cook-image
cook-image:
	git rev-parse HEAD > version
	docker build -t $(DOCKER_TAG) .

.PHONY: cook-image-external
cook-image-external:
	git rev-parse HEAD > version
	docker build -t $(DOCKER_TAG) -f Dockerfile.external .

.PHONY: completions
completions: virtualenv_run
	mkdir -p completions
	virtualenv_run/bin/static_completion clusterman bash --write-vendor-directory $@
	virtualenv_run/bin/static_completion clusterman zsh --write-vendor-directory $@
	virtualenv_run/bin/static_completion clusterman fish --write-vendor-directory $@

.PHONY: install-hooks
install-hooks: virtualenv_run
	./virtualenv_run/bin/pre-commit install -f --install-hooks

virtualenv_run: $(VIRTUALENV_RUN_REQUIREMENTS)
	tox -e $(VIRTUALENV_RUN_TARGET)

.PHONY: version-bump
version-bump:
	@set -e; \
	if [ -z ${EDITOR} ]; then \
		echo "EDITOR environment variable not set, please set and try again"; \
		false; \
	fi; \
	OLD_PACKAGE_VERSION=$$(python setup.py --version); \
	${EDITOR} ${PKG_NAME}/__init__.py; \
	PACKAGE_VERSION=$$(python setup.py --version); \
	if [ "$${OLD_PACKAGE_VERSION}" = "$${PACKAGE_VERSION}" ]; then \
		echo "package version unchanged; aborting"; \
		false; \
	elif [ ! -f debian/changelog ]; then \
		dch -v $${PACKAGE_VERSION} --create --package=$(PKG_NAME) -D "xenial bionic" -u low ${ARGS}; \
	else \
		dch -v $${PACKAGE_VERSION} -D "xenial bionic" -u low ${ARGS}; \
	fi; \
	git add debian/changelog ${PKG_NAME}/__init__.py; \
	set +e; git commit -m "Bump to version $${PACKAGE_VERSION}"; \
	if [ $$? -ne 0 ]; then \
		git add debian/changelog ${PKG_NAME}/__init__.py; \
		git commit -m "Bump to version $${PACKAGE_VERSION}"; \
	fi; \
	if [ $$? -eq 0 ]; then git tag "v$${PACKAGE_VERSION}"; fi

dist:
	ln -sf package/dist ./dist

itest_%: dist completions
	tox -e acceptance
	make -C package $@
	./.tox/acceptance/bin/docker-compose -f acceptance/docker-compose.yaml down

.PHONY:
package: itest_xenial itest_bionic

.PHONY:
clean: clean-cache
	-docker-compose -f acceptance/docker-compose.yaml down
	-rm -rf docs/build
	-rm -rf virtualenv_run/
	-rm -rf .tox
	-unlink dist
	-rm -rf package/dist/*

clean-cache:
	find -name '*.pyc' -delete
	find -name '__pycache__' -delete
	rm -rf .mypy_cache
	rm -rf .pytest_cache

.PHONY:
debug:
	docker build . -t clusterman_debug_container
	paasta_docker_wrapper run -it \
		-v $(shell pwd)/clusterman:/code/clusterman:rw \
		-v $(shell pwd)/.cman_debug_bashrc:/home/nobody/.bashrc:ro \
		-v /nail/srv/configs:/nail/srv/configs:ro \
		-v /nail/etc/services:/nail/etc/services:ro \
		-v /etc/boto_cfg:/etc/boto_cfg:ro \
		-e "CMAN_CLUSTER=mesosstage" \
		-e "CMAN_POOL=default" \
		clusterman_debug_container /bin/bash

.PHONY:
upgrade-requirements:
	upgrade-requirements --python python3.7
