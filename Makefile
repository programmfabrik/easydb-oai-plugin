PLUGIN_NAME = oai
INSTALL_FILES = \
	src/server \
	oai.yml

all: build

include ../../easydb-library/tools/base-plugins.make

build: code

code:

clean: clean-base

wipe: wipe-base