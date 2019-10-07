.PHONY: _default clean clean-libuv distclean compile debug docs test testinstalled release setup-build ci-clean


PYTHON ?= python


_default: compile


clean:
	rm -fr dist/ docs/_build/ *.egg-info
	find asynckafka tests -name '*.so' -type f -delete
	find asynckafka tests -name '*.c' -type f -delete
	find . -name '__pycache__' -type f -delete


setup-build:
	$(PYTHON) setup.py build_ext --inplace --tests


compile: clean setup-build


docs:
	pip install . --no-cache --upgrade
	sphinx-autobuild -b html ./docs ./docs/_build/html


test: compile
	docker-compose up -d
	sleep 20
	python -m unittest tests.asynckafka_tests -v
