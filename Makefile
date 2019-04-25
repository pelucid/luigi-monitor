################## BOILER PLATE CHECKS ##########

# Guard against running Make commands outside a virtualenv
venv:
ifndef VIRTUAL_ENV
$(error VIRTUALENV is not set - please activate environment)
endif

############### PUBLIC API #############


test:
	pytest tests/

deps: venv
	pip install --upgrade pip==18.1
	pip install -r requirements.txt
