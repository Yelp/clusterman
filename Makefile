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

# `mm` will make development
.PHONY: minimal
minimal: development

.PHONY: docs
docs:
	-rm -rf docs/build
	tox -e docs

.PHONY: upload_docs
upload_docs: docs
	tox -e upload_docs

.PHONY: dev_docs
dev_docs: docs
	PATH=$(PWD)/virtualenv_run/bin:$(PATH) serve-dev-servicedocs

.PHONY: mypy
mypy:
	tox -e mypy

.PHONY: test
test: clean-cache mypy
	tox

.PHONY: itest
itest: cook-image
	tox -e acceptance
	./paasta-itest-runner spot_price_collector "--aws-region=us-west-1 --disable-sensu"
	./paasta-itest-runner cluster_metrics_collector "--cluster=docker --env-config-path acceptance/srv-configs/clusterman.yaml --cluster-config-dir acceptance/srv-configs/clusterman-clusters --disable-sensu"
	./paasta-itest-runner autoscaler_bootstrap "--env-config-path acceptance/srv-configs/clusterman.yaml --cluster-config-dir acceptance/srv-configs/clusterman-clusters" autoscaler

.PHONY: cook-image
cook-image:
	git rev-parse HEAD > version
	docker build -t $(DOCKER_TAG) .

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
clean:
	-docker-compose -f acceptance/docker-compose.yaml down
	-rm -rf docs/build
	-rm -rf virtualenv_run/
	-rm -rf .tox
	-unlink dist
	-find . -name '*.pyc' -delete
	-find . -name '__pycache__' -delete
	-rm -rf package/dist/*

clean-cache:
	find -name '*.pyc' -delete
	find -name '__pycache__' -delete

.PHONY:
upgrade-requirements:
	upgrade-requirements -i https://pypi.yelpcorp.com/simple --pip-tool pip-custom-platform --install-deps pip-custom-platform

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
