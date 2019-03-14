SHELL=/bin/bash
PYTHON=$(shell which python3)

check:
	if [ "$(PYTHON)" == "" ]; then "Python3 not found"; exit 1; fi

env: check 
	apt-get update && apt-get install -y --allow-unauthenticated inotify-tools
	pip3 install virtualenv
	virtualenv -p $(PYTHON) ./env && source ./env/bin/activate && pip install -r deps.txt

test:
	source ./env/bin/activate && python -m unittest

develop: check
	while inotifywait -r -e create ./statwolf --exclude ''\\.pyc$$''; do make test; done

examples: check
	source ./env/bin/activate && for SCRIPT in ./examples/*.py; do PYTHONPATH="." python $$SCRIPT; done

.PHONY: env develop check
