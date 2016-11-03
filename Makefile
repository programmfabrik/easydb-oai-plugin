PLUGIN_NAME = oai
INSTALL_FILES = \
	src/server \
	oai.yml

all: build

include ../base-plugins.make

build: code

code: $(L10N)

clean: clean-base
