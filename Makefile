extract:
	@python ./src/etl/extract.py

transform:
	@python ./src/etl/transform.py

load:
	@python ./src/etl/load.py

etl: extract transform load