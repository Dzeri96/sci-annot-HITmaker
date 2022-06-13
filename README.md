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
3. Create the qualification types: `python3 manage_HITs.py -vv -e ENVFILE create-qual-types`

## Usage
To show the usage of this library, run
```
python3 manage_HITs.py --help
```
A typical workflow would look like this:

1. Publish some HITs: `python3 manage_HITs.py -v -e ENVFILE publish-random 50 -c "Test"`
2. Fetch the results after some time: `python3 manage_HITs.py -v -e ENVFILE fetch-results`
3. Automatically evaluate fetched results if possible: `python3 manage_HITs.py -v -e ENVFILE eval-retrieved`

### URL parameters
These parameters change the behavior of the front-end:

    - 'crop_whitespace' - Boolean param, shows a cropped version of the bounding boxes, but doesn't actually change them on the back-end.
    - 'page_status' - When an action on a page is performed, selects the status of the next page to be loaded. For example `page_status=DEFERRED`.

## TODO
- Write about the security implications of using XML parsers, and how MTurk requesters' servers could be prime targets.
They have money after all
- Implement race condition handling in the API