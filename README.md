# Trevas-Batch

Batch launcher for the Trevas VTL engine, building metrics, reports.

## Execution

Trevas Batch can be executed in a Kubernetes cluster.

Kubernetes object sample definitions are available in `.kubernetes` folder.

Fill `config-map` values and apply.

## Configuration

### Environment variables

- `config.path`: JSON configuration file location (see next section)
- `report.path`: Specify a path to publish a Markdown report of the batch.

### Input configuration file

```json
{
	"inputs": [
		{
			"name": "ds1",
			"format": "csv",
			"location": "..."
		},
        {
            "name": "ds2",
            "format": "parquet",
            "location": "..."
        },
        {
            "name": "ds3",
            "format": "jdbc",
            "user": "...",
            "password": "...",
            "url": "...",
            "dbType": "postgre",
            "query": "SELECT * FROM my_table"
        }
	],
	"outputs": [
		{
			"name": "ds_out",
			"location": "..."
		}
	],
	"script": "ds_out := ds1 + ds2 + ds3;"
}
```

- `format`: csv, parquet, sas, jdbc 
- `dbType`: postgre, mariadb

_Output datasets will necessarily be written in `parquet`._