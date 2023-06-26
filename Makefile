.PHONY: venv install run add remove clean
.DEFAULT_GOAL := run

VENV = venv
PYTHON = $(VENV)/bin/python3
PIP = $(VENV)/bin/pip

venv:
	python3 -m venv venv

install:
	. ${VENV}/bin/activate;	$(PIP) install -r requirements.txt

run:
	. ${VENV}/bin/activate; $(PYTHON) process.py

add:
	. ${VENV}/bin/activate;	$(PIP) install $(package); $(PIP) freeze --all > requirements.txt

remove:
	. ${VENV}/bin/activate;	$(PIP) uninstall -y $(package); $(PIP) freeze --all > requirements.txt

clean:
	rm -rf $(VENV)
	find . -type f -name '*.pyc' -delete
