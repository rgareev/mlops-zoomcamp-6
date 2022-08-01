all: integration-tests

unit-tests:
	pipenv run pytest tests

integration-tests: unit-tests
	./integration-test.sh