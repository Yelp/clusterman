.PHONY: itest_% package_% test_% build_%_docker clean

SYSTEM_PKG_NAME ?= clusterman
PYTHON_PKG_NAME ?= $(shell cd $(CURDIR)/.. && python setup.py --name)
PACKAGE_VERSION ?= $(shell cd $(CURDIR)/.. && python setup.py --version)

UID:=`id -u`
GID:=`id -g`
DOCKER_BUILD_RUN:=docker run -t -v $(CURDIR)/..:/src:ro -v $(CURDIR)/dist:/dist:rw
DOCKER_ITEST_RUN:=docker run -t -v $(CURDIR)/itest:/itest:rw -v $(CURDIR)/dist:/dist:ro --net=acceptance_default \
	-v $(CURDIR)/../acceptance/run_instance.py:/itest/run_instance.py:ro \
	-v $(CURDIR)/../acceptance/autoscaler_config.tmpl:/itest/autoscaler_config.tmpl:ro \
	-v $(CURDIR)/../acceptance/srv-configs:/nail/srv/configs:ro \
	-v $(CURDIR)/../acceptance/clusterman.sh:/etc/boto_cfg/clusterman.sh:ro \
	-v $(CURDIR)/../acceptance/clusterman.json:/etc/boto_cfg/clusterman.json:ro \
	-v $(CURDIR)/../acceptance/clusterman.json:/etc/boto_cfg/clusterman_metrics.json:ro
VERSIONED_FILES:=$(shell cd $(CURDIR)/.. && git ls-files -z --cached --modified | xargs -0 -I@ echo -n "'@' ")
DOCKER_WORKDIR:=mkdir -p /work && cd /src && cp -vP --parents $(VERSIONED_FILES) /work/ && cp -r completions /work/ && cd /work

itest_%: package_% dist/%/Packages.gz
	$(DOCKER_ITEST_RUN) \
		-v /nail/etc/services/services.yaml:/nail/etc/services/services.yaml:ro \
		-v $(CURDIR)/../acceptance/$*/clusterman_signals_acceptance.tar.gz:/itest/$*/clusterman_signals_acceptance.tar.gz \
		ubuntu:$* /itest/ubuntu.sh $(SYSTEM_PKG_NAME) $(PACKAGE_VERSION)

itest_%-external: package_%_external dist/%/Packages.gz
	$(DOCKER_ITEST_RUN) \
		-v $(CURDIR)/../acceptance/$*/clusterman_signals_acceptance.tar.gz:/itest/$*/clusterman_signals_acceptance.tar.gz \
		-v $(CURDIR)/../acceptance/srv-configs/clusterman-external.yaml:/nail/srv/configs/clusterman.yaml:ro \
		ubuntu:$* /itest/ubuntu.sh $(SYSTEM_PKG_NAME) $(PACKAGE_VERSION)

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
		&& dpkg-buildpackage -d -Rdebian/rules.external \
		&& mv ../*.deb /dist/$*/$(SYSTEM_PKG_NAME)_$(PACKAGE_VERSION)_amd64.deb \
		&& chown $(UID):$(GID) /dist/$*/*.deb"

build_%_docker:
	mkdir -p dist
	cd dockerfiles/$*/ && docker build -t "$(SYSTEM_PKG_NAME)_$*_container" .
