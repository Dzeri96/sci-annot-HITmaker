# MTurk HIT Client

Amazon Mechanical Turk Client written in python for managing HITs of the task of annotating elements in scientific publications.

## Prerequisites
- python3
- pip
- pipenv

To install the dependencies, run 'pipenv install'

## First time setup
Every time you want to start work in a new environment (there are basically only two), you have to follow these steps:

1. Create the main HIT type: `python3 manage_HITs.py --env ENVFILE create-hit-type -a`
2. Ingest PDFs from parquet file: `python3 manage_HITs.py -vv -e ENVFILE ingest DOWNLOADED_PDFS_PARQ`

## Usage
To show the usage of this library, run
```
python3 manage_HITs.py --help
```

## TODO
- Write about the security implications of using XML parsers, and how MTurk requesters' servers could be prime targets.
They have money after all

- Add asyncio to MTurk API calls
- Implement race condition handling in the API