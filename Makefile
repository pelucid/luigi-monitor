CODEARTIFACT_AUTH_TOKEN := $(shell aws codeartifact get-authorization-token --domain growthintelligence --domain-owner 048965452656 --query authorizationToken --output text)

install:
	poetry config http-basic.gi-pypi aws $(CODEARTIFACT_AUTH_TOKEN)
	poetry install --no-interaction --no-ansi

test:
	pytest --cov-report term-missing --cov-report html --cov=luigi-monitor --cov-branch tests/

build:
	poetry build

publish: build
	poetry config repositories.gi-pypi-publish  https://growthintelligence-048965452656.d.codeartifact.eu-west-1.amazonaws.com/pypi/gi-pypi/
	poetry publish --repository gi-pypi-publish --username aws --password $(CODEARTIFACT_AUTH_TOKEN)