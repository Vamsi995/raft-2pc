.DEFAULT_GOAL := build

.PHONY: run kill 

kill:
	bash scripts/teardown.sh

run:
	bash scripts/start_servers.sh

clean: 
	bash scripts/clean.sh