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

.PHONY: docs-server
docs-server: docs
	python docs/doc_server.py

.PHONY: test
test: clean-cache
	tox

.PHONY: itest
itest: cook-image
	paasta local-run -s clusterman -c norcal-devc -i spot_prices --healthcheck-only
	paasta local-run -s clusterman -c norcal-devc -i cluster_metrics --healthcheck-only
	paasta local-run -s clusterman -c norcal-devc -i autoscaler --healthcheck-only

.PHONY: cook-image
cook-image:
	git rev-parse HEAD > version
	docker build -t $(DOCKER_TAG) .

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
		dch -v $${PACKAGE_VERSION} --create --package=$(PKG_NAME) -D trusty -u low ${ARGS}; \
	else \
		dch -v $${PACKAGE_VERSION} -D trusty -u low ${ARGS}; \
	fi; \
	git add debian/changelog ${PKG_NAME}/__init__.py; \
	set +e; git commit -m "Bump to version $${PACKAGE_VERSION}"; \
	if [ $$? -ne 0 ]; then \
		git add debian/changelog ${PKG_NAME}/__init__.py; \
		git commit -m "Bump to version $${PACKAGE_VERSION}"; \
	fi; \
	if [ $$? -eq 0 ]; then git tag "v$${PACKAGE_VERSION}"; fi

dist: development
	ln -sf yelp_package/dist ./dist

itest_%: dist
	make -C yelp_package $@

.PHONY:
package: itest_trusty itest_xenial

.PHONY: clean
clean:
	-rm -rf docs/build
	-rm -rf virtualenv_run/
	-rm -rf .tox
	-unlink dist
	-find . -name '*.pyc' -delete
	-find . -name '__pycache__' -delete
	-rm -rf yelp_package/dist/*

clean-cache:
	find -name '*.pyc' -delete
	find -name '__pycache__' -delete
