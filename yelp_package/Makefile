.PHONY: itest_% package_% test_% build_%_docker clean

SYSTEM_PKG_NAME ?= clusterman
PYTHON_PKG_NAME ?= $(shell cd $(CURDIR)/.. && python setup.py --name)
PACKAGE_VERSION ?= $(shell cd $(CURDIR)/.. && python setup.py --version)

UID:=`id -u`
GID:=`id -g`
DOCKER_BUILD_RUN:=docker run -t -v $(CURDIR)/..:/src:ro -v $(CURDIR)/dist:/dist:rw
DOCKER_ITEST_RUN:=docker run -t -v $(CURDIR)/itest:/itest:rw -v $(CURDIR)/dist:/dist:ro --net=acceptance_default \
	-v $(CURDIR)/../acceptance/srv-configs:/nail/srv/configs:ro \
	-v $(CURDIR)/../acceptance/clusterman.sh:/etc/boto_cfg/clusterman.sh:ro \
	-v $(CURDIR)/../acceptance/clusterman.json:/etc/boto_cfg/clusterman.json:ro \
	-v $(CURDIR)/../acceptance/clusterman.json:/etc/boto_cfg/clusterman_metrics.json:ro
VERSIONED_FILES:=$(shell cd $(CURDIR)/.. && git ls-files -z --cached --modified | xargs -0 -I@ echo -n "'@' ")
DOCKER_WORKDIR:=mkdir -p /work && cd /src && cp -v --parents $(VERSIONED_FILES) /work/ && cd /work

itest_%: package_% dist/%/Packages.gz
	$(DOCKER_ITEST_RUN) \
		-v $(CURDIR)/../acceptance/$*/clusterman_signals_acceptance.tar.gz:/itest/clusterman_signals_acceptance.tar.gz \
		docker-dev.yelpcorp.com/$*_yelp \
		/itest/ubuntu.sh $(SYSTEM_PKG_NAME) $(PACKAGE_VERSION)

dist/%/Packages.gz:
	$(DOCKER_BUILD_RUN) $(SYSTEM_PKG_NAME)_$*_container /bin/bash -c "\
	  cd /dist/$* && dpkg-scanpackages . /dev/null | gzip -9c > Packages.gz \
	  && chown $(UID):$(GID) Packages.gz"

package_%: build_%_docker
	mkdir -p $(CURDIR)/dist/$*
	$(DOCKER_BUILD_RUN) $(SYSTEM_PKG_NAME)_$*_container /bin/bash -c "\
	    $(DOCKER_WORKDIR) \
	    && dpkg-buildpackage -d \
	    && mv ../*.deb /dist/$*/$(SYSTEM_PKG_NAME)_$(PACKAGE_VERSION)_amd64.deb \
	    && chown $(UID):$(GID) /dist/$*/*.deb"

build_%_docker:
	mkdir -p dist
	cd dockerfiles/$*/ && docker build -t "$(SYSTEM_PKG_NAME)_$*_container" .
