# MTurk HIT Client

Amazon Mechanical Turk Client written in python for managing HITs of the task of annotating elements in scientific publications.

## Prerequisites
- python3
- pip
- pipenv

To install the dependencies, run 'pipenv install'

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