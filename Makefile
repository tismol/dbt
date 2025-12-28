.PHONY: deps seed run test unit docs lint fix clean

deps:
	dbt deps

seed:
	dbt seed

run:
	dbt run

test:
	dbt test

unit:
	dbt test --select test_type:unit

docs:
	dbt docs generate
	dbt docs serve

lint:
	sqlfluff lint models/marts

fix:
	sqlfluff fix models/marts

clean:
	dbt clean
