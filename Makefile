PLUGIN_NAME = oai
INSTALL_FILES = \
	$(WEB)/l10n/cultures.json \
	$(WEB)/l10n/de-DE.json \
	$(WEB)/l10n/en-US.json \
	$(WEB)/l10n/es-ES.json \
	$(WEB)/l10n/it-IT.json \
	src/server \
	oai.yml

L10N_FILES = l10n/oai.csv
L10N_GOOGLE_KEY = 1glXObMmIUd0uXxdFdiPWRZPLCx6qEUaxDfNnmttave4
L10N_GOOGLE_GID = 425602350
L10N2JSON = python ../../library/tools/l10n2json.py

all: build

include ../../library/tools/base-plugins.make

build: code

code: $(L10N)

clean: clean-base

wipe: wipe-base