# Aqueduct point analysis microservice

[![Build Status](https://travis-ci.com/resource-watch/aqueduct-analysis-microservice.svg?branch=dev)](https://travis-ci.com/resource-watch/aqueduct-analysis-microservice)
[![Test Coverage](https://api.codeclimate.com/v1/badges/412dbad07a559dbd4105/test_coverage)](https://codeclimate.com/github/resource-watch/aqueduct-analysis-microservice/test_coverage)

## Dependencies

Dependencies on other Microservices:

- [Geostore](https://github.com/gfw-api/gfw-geostore-api)
- [Control Tower](git@github.com:control-tower/control-tower.git)

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

3. Run the aqueduct.sh shell script in development mode.

```shell
./aqueduct.sh develop
```

or with the auxillery services included:

```shell
./aqueduct.sh gr_develop
```

If this is the first time you run it, it may take a few minutes.

### Database init

In order to populate the DB you will need to place a database dump in the
`/data` folder and source it from within the postgres container.

* Copy the dump (assuming `flood_v3.dump`) into ./data from host machine (your laptop)

* Ensure the database is running (containerized)
  docker-compose -f docker-compose-gr.yml up aqueduct-postgres

* Get a shell prompt on the database container
  docker exec -it aqueduct-postgres-gr /bin/bash

**** change to the data directory
  cd /docker-entrypoint-initdb.d

* Import data
  pg_restore -1 -U postgres -d flood_v2 flood_v3.dump

### Code structure

The API has been packed in a Python module (aqueduct). It creates and exposes a WSGI application. The core functionality
has been divided in three different layers or submodules (Routes, Services and Models).

There are also some generic submodules that manage the request validations, HTTP errors and the background tasks manager.

### Deploy

merge into dev
deploy remote is the one watched by Jenkins
git push deploy dev:dev
https://jenkins.aws-dev.resourcewatch.org/me/my-views/view/all/

### Running

  * Load database (get from Todd)
  * aqueduct.sh gr_develop
  * curl http://localhost:5100/api/v1/aqueduct/analysis | jq is the base url. This should give you a 400.
  * aqueduct/routes/api/v1/ps_router.py has routing information
  * curl http://localhost:5100/api/v1/aqueduct/analysis/cba/widget/1 | jq I believe connects to change in #177628334
