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
development: virtualenv_run docker-venv install-hooks

# `mm` will make development
.PHONY: minimal
minimal: development

.PHONY: docs
docs:
	tox -e docs

.PHONY: test
test: clean-cache
	tox

.PHONY: acceptance
acceptane: clean-cache
	tox -e acceptance

.PHONY: itest
itest:
	paasta local-run --service clusterman --cluster norcal-devc --instance main --healthcheck

.PHONY: cook-image
cook-image:
	git rev-parse HEAD > version
	docker build -t $(DOCKER_TAG) .

.PHONY: install-hooks
install-hooks: virtualenv_run
	./virtualenv_run/bin/pre-commit install -f --install-hooks

docker-venv: virtualenv_run
	@# for yelp-compose dev-mode
	virtualenv_run/bin/docker-run-passthrough --passthrough-host-pip-faster-cache \
			--volume ./docker-venv:/code/virtualenv_run \
			docker-dev.yelpcorp.com/xenial_pkgbuild:latest -- \
			tox -e virtualenv_run-dev

virtualenv_run: $(VIRTUALENV_RUN_REQUIREMENTS)
	@# See https://confluence.yelpcorp.com/display/~asottile/GettingPythonOffLucid
	@# and https://migration-status.dev.yelp.com/metric/ToxNonLucid
	@# for more information (e.g., using pip-custom-platform, tox virtualenv build, etc)
	tox -e $(VIRTUALENV_RUN_TARGET)

.PHONY: clean
clean:
	rm -rf docs/build
	rm -rf virtualenv_run/
	rm -rf .tox
	find . -name '*.pyc' -delete
	find . -name '__pycache__' -delete

clean-cache:
	find -name '*.pyc' -delete
	find -name '__pycache__' -delete

.PHONY: push-swagger-spec-to-registry
push-swagger-spec-to-registry:
	swagger post clusterman api_docs/swagger.json
