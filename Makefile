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
	$(MAKE) -C docs html


test:
	python -m unittest tests.asynckafka_tests -v
