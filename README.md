# Aqueduct point analysis microservice

[![Build Status](https://travis-ci.org/resource-watch/aqueduct-analysis-microservice.svg?branch=master)](https://travis-ci.org/resource-watch/aqueduct-analysis-microservice)
[![Test Coverage](https://api.codeclimate.com/v1/badges/412dbad07a559dbd4105/test_coverage)](https://codeclimate.com/github/resource-watch/aqueduct-analysis-microservice/test_coverage)

## Getting started

### Requirements

You need to install Docker in your machine if you haven't already [Docker](https://www.docker.com/)

### Development

Follow the next steps to set up the development environment in your machine.

1. Clone the repo and go to the folder

```ssh
git clone https://github.com/resource-watch/aqueduct-analysis-microservice
cd aqueduct-analysis-microservice
```


2. Copy `.env.sample` to `.env` and set the corresponding variable values

```ssh
./aqueduct.sh develop
```

3. Run the aqueduct.sh shell script in development mode.

```ssh
./aqueduct.sh develop
```

If this is the first time you run it, it may take a few minutes.

### Code structure

The API has been packed in a Python module (aqueduct). It creates and exposes a WSGI application. The core functionality
has been divided in three different layers or submodules (Routes, Services and Models).

There are also some generic submodules that manage the request validations, HTTP errors and the background tasks manager.


### Testing

To run tests, you need to install both production and development dependencies. To do so, run:

```ssh
pip install -r requirements.txt
pip install -r requirements_dev.txt
```

To run the test suite, you'll need to configure some environment variables:
- `CT_URL`: Can have any value, as long as it's a syntactically correct URL.
- `CT_TOKEN`: Can have any value
- `POSTGRES_URL`: Full URL of a running PostgreSQL server with a preloaded database. Example: `postgresql://postgres@localhost:5432/aqueduct_ms_test`