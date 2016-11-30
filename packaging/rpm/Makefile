PACKAGE_NAME?=  redborder-samza-bi

VERSION?= $(shell git describe --abbrev=6 --tags HEAD --always | sed 's/-/_/g' | sed 's/^rb_samza_bi_//')

TAG_VERSION?= $(shell env ruby get_version.rb | sed 's/^\([^-]*\)-.*/\1/')

BUILD_NUMBER?= 1

MOCK_CONFIG?=default

RESULT_DIR?=pkgs

#all: rpm
all: rpm


SOURCES:
	mkdir -p SOURCES

archive: SOURCES
	cd ../../ && \
	git archive --prefix=$(PACKAGE_NAME)-$(VERSION)/ \
                -o packaging/rpm/SOURCES/$(PACKAGE_NAME)-$(VERSION).tar.gz HEAD

build_prepare: archive
	mkdir -p $(RESULT_DIR)
	rm -f $(RESULT_DIR)/$(PACKAGE_NAME)*.rpm
	which sbt &>/dev/null || ( echo "sbt build tool not found" && exit 1 )
	which mvn &>/dev/null || ( echo "maven not found" && exit 1 )
	env ruby get_version.rb &>/dev/null || ( echo "ruby or xml-simple gem not found" && exit 1 )
	cd ../../ && bash packaging/rpm/prepare_pack.sh && cp target/rb-samza-*-dist.tar.gz packaging/rpm/SOURCES/

srpm: build_prepare
	/usr/bin/mock \
            -r $(MOCK_CONFIG) \
            --define "__version $(VERSION)" \
            --define "__release $(BUILD_NUMBER)" \
            --define "__tagversion $(TAG_VERSION)" \
            --resultdir=$(RESULT_DIR) \
            --buildsrpm \
            --spec=${PACKAGE_NAME}.spec \
            --sources=SOURCES
	@echo "======= Source RPM now available in $(RESULT_DIR) ======="

rpm: srpm
	/usr/bin/mock \
            -r $(MOCK_CONFIG) \
            --define "__version $(VERSION)"\
            --define "__release $(BUILD_NUMBER)"\
            --define "__tagversion $(TAG_VERSION)" \
            --resultdir=$(RESULT_DIR) \
            --rebuild $(RESULT_DIR)/$(PACKAGE_NAME)*.src.rpm
	@echo "======= Binary RPMs now available in $(RESULT_DIR) ======="

clean:
	rm -rf SOURCES pkgs

distclean: clean
	rm -f build.log root.log state.log available_pkgs installed_pkgs \
            *.rpm *.tar.gz


