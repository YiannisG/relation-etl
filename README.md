# Relation ETL

This package provides a way to represent gene, transcriptomic and exon data served via the mock API in a relational database. It also provides a local Airflow deployment to aid visually with task execution and debugging.

## Requirements

- First launch the docker container with the mock-API (assumed on port 8000)
- Add a value for **AIRFLOW_API_AUTH_JWT_SECRET** to the **.env** file. Modify file as needed.

## Running the service

```bash

docker compose up -d --build

# to teardown completely
docker compose down -v --remove-orphans
```
The service should be accessible via: http://127.0.0.1:8080. 
ETL tasks can be launched via the Airflow UI.

## Explanation

### Choice of data modelling
The mock data in the API is fairly structured as everything consisted of lists of dictionaries with mostly similar fields. Whilst not all fields were included in every entry the dataset overall was consistent enough to model predictably. This provides the opportunity for well-defined curation of the data as part of transformation e.g. combining fields where multiple primary keys exist in a predictable way, enriching the value of this data collection and fast downstream querying with simple SQL.

With the SQLite database engine we can store this in a single-file database locally.

### Data cleaning
There were numerous cases where entries required normalisation, deduplication, exclusion, or fields needed merging. These rules are clearly laid out in the code:
- Where exon sizes exceeded a maximum sensible value they were excluded
- for transcripts I noticed the field **is_canonical** and thought it made sense where there is a duplicate entry to prioritise the entry with this field set to **true**
- The approach also prioritised keeping data which had strong relationships e.g. if a transcript did not map to a gene_id that we already had it was excluded.

Where data was excluded the **etl_quarantine** table recorded why for each entry so it can be interrogated further.

For this particular dataset:
- for extraction there remained 15 genes, 18 transcripts, 31 exons (3 http requests, 0 retries)
- at tranformation there were 12 genes, 15 transcripts, 24 exons kept; 7 quarantined; 17 merge conflicts logged

### Difficulties
- Typos in setting up Airflow with docker compose and working my way through the UI until I got the hang of it.
- Deciding what to prioritise in how this data is captured e.g. do we want to capture all data or are we happy to drop data that is not connected?
