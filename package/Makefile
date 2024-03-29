.PHONY: itest_% itest_%-external package_% package_%-external build_%_docker

SYSTEM_PKG_NAME ?= clusterman
PYTHON_PKG_NAME ?= $(shell cd $(CURDIR)/.. && python setup.py --name)
PACKAGE_VERSION ?= $(shell cd $(CURDIR)/.. && python setup.py --version)
ACCEPTANCE_DIR:=$(CURDIR)/../acceptance

ifeq ($(findstring .yelpcorp.com,$(shell hostname -f)), .yelpcorp.com)
	PAASTA_ENV ?= YELP
else
	PAASTA_ENV ?= $(shell hostname --fqdn)
endif

ifeq ($(PAASTA_ENV),YELP)
	export DOCKER_REGISTRY ?= docker-dev.yelpcorp.com
	export BIONIC_IMAGE_NAME ?= bionic_pkgbuild
	export JAMMY_IMAGE_NAME ?= jammy_pkgbuild
else
	export DOCKER_REGISTRY ?= docker.io
	export BIONIC_IMAGE_NAME ?= ubuntu:bionic
	export JAMMY_IMAGE_NAME ?= ubuntu:jammy
endif

UID:=`id -u`
GID:=`id -g`
DOCKER_BUILD_RUN:=docker run -t -e CI=${CI} -e PAASTA_ENV=${PAASTA_ENV} -v $(CURDIR)/..:/src:ro -v $(CURDIR)/dist:/dist:rw
VERSIONED_FILES:=$(shell cd $(CURDIR)/.. && git ls-files -z --cached --modified | xargs -0 -I@ echo -n "'@' ")
DOCKER_WORKDIR:=mkdir -p /work && cd /src && cp -vP --parents $(VERSIONED_FILES) /work/ && cp -r completions /work/ && cd /work

itest_%: export EXTRA_VOLUME_MOUNTS=/nail/etc/services/services.yaml:/nail/etc/services/services.yaml:ro
itest_%: package_% dist/%/Packages.gz
	./debian-itest-runner $* $(SYSTEM_PKG_NAME) $(PACKAGE_VERSION) $(PAASTA_ENV)

itest_%-external: export EXTRA_VOLUME_MOUNTS=$(ACCEPTANCE_DIR)/srv-configs/clusterman-external.yaml:/nail/srv/configs/clusterman.yaml:ro
itest_%-external: package_%_external dist/%/Packages.gz
	./debian-itest-runner $* $(SYSTEM_PKG_NAME) $(PACKAGE_VERSION) $(PAASTA_ENV)

dist/%/Packages.gz:
	$(DOCKER_BUILD_RUN) $(SYSTEM_PKG_NAME)_$*_container /bin/bash -c "\
	  cd /dist/$* && dpkg-scanpackages . /dev/null | gzip -9c > Packages.gz \
	  && chown $(UID):$(GID) Packages.gz"

package_%: build_%_docker
	mkdir -p $(CURDIR)/dist/$*
	$(DOCKER_BUILD_RUN) \
		$(SYSTEM_PKG_NAME)_$*_container /bin/bash -c "\
		$(DOCKER_WORKDIR) && cat extra-requirements-yelp.txt >> requirements.txt \
		&& dpkg-buildpackage -d \
		&& mv ../*.deb /dist/$*/$(SYSTEM_PKG_NAME)_$(PACKAGE_VERSION)_amd64.deb \
		&& chown $(UID):$(GID) /dist/$*/*.deb"

package_%_external: build_%_docker
	mkdir -p $(CURDIR)/dist/$*
	$(DOCKER_BUILD_RUN) \
		$(SYSTEM_PKG_NAME)_$*_container /bin/bash -c "\
		$(DOCKER_WORKDIR) && echo \"file:///work/examples/clusterman_metrics\" >> requirements.txt \
		&& dpkg-buildpackage -d \
		&& mv ../*.deb /dist/$*/$(SYSTEM_PKG_NAME)_$(PACKAGE_VERSION)_amd64.deb \
		&& chown $(UID):$(GID) /dist/$*/*.deb"

build_%_docker:
	mkdir -p dist
	cd dockerfiles/$*/ && docker build --build-arg DOCKER_REGISTRY=${DOCKER_REGISTRY} --build-arg IMAGE_NAME=$($(shell echo '$*' | tr '[:lower:]' '[:upper:]')_IMAGE_NAME) -t "$(SYSTEM_PKG_NAME)_$*_container" .
