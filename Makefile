DOCKER_TAG ?= clusterman-dev-$(USER)
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
	tox -e docs

.PHONY: test
test: clean-cache
	tox

.PHONY: itest
itest: cook-image
	paasta local-run --service clusterman --cluster norcal-devc --instance main

.PHONY: cook-image
cook-image:
	git rev-parse HEAD > version
	docker build -t $(DOCKER_TAG) .

.PHONY: install-hooks
install-hooks: virtualenv_run
	./virtualenv_run/bin/pre-commit install -f --install-hooks

virtualenv_run: $(VIRTUALENV_RUN_REQUIREMENTS)
	@# See https://confluence.yelpcorp.com/display/~asottile/GettingPythonOffLucid
	@# and https://migration-status.dev.yelp.com/metric/ToxNonLucid
	@# for more information (e.g., using pip-custom-platform, tox virtualenv build, etc)
	tox -e $(VIRTUALENV_RUN_TARGET)

# debian package info
SYSTEM_PKG_NAME=clusterman
PYTHON_PKG_NAME=$(shell python setup.py --name)

.PHONY: changelog
version-bump:
	@ set -e; \
	OLD_PACKAGE_VERSION=$$(python setup.py --version); \
	if [ -z ${EDITOR} ]; then \
		echo "EDITOR environment variable not set, please set and try again"; \
		false; \
	fi; \
	${EDITOR} ${PYTHON_PKG_NAME}/__init__.py; \
	PACKAGE_VERSION=$$(python setup.py --version); \
	if [ "$${OLD_PACKAGE_VERSION}" = "$${PACKAGE_VERSION}" ]; then \
		echo "package version unchanged; aborting"; \
		false; \
	else if [ ! -f debian/changelog ]; then \
		dch -v $${PACKAGE_VERSION} --create --package=$(SYSTEM_PKG_NAME) -D trusty -u low ${ARGS}; \
	else \
		dch -v $${PACKAGE_VERSION} -D trusty -u low ${ARGS}; \
	fi; fi; \
	git add debian/changelog ${PYTHON_PKG_NAME}/__init__.py; \
	set +e; git commit -m "Bump to version $${PACKAGE_VERSION}"; \
	if [ $$? ]; then git commit -a -m "Bump to version $${PACKAGE_VERSION}"; fi; \
	if [ $$? ]; then git tag "v$${PACKAGE_VERSION}"; fi



dist: development
	ln -sf yelp_package/dist ./dist

itest_%: dist
	make -C yelp_package $@

.PHONY:
package: itest_trusty itest_xenial

.PHONY: clean
clean:
	rm -rf docs/build
	rm -rf virtualenv_run/
	rm -rf .tox
	unlink dist
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete

clean-cache:
	find -name '*.pyc' -delete
	find -name '__pycache__' -delete
