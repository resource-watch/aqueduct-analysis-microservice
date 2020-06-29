# Aqueduct point analysis microservice

[![Build Status](https://travis-ci.org/resource-watch/aqueduct-analysis-microservice.svg?branch=develop)](https://travis-ci.org/resource-watch/aqueduct-analysis-microservice)
[![Test Coverage](https://api.codeclimate.com/v1/badges/412dbad07a559dbd4105/test_coverage)](https://codeclimate.com/github/resource-watch/aqueduct-analysis-microservice/test_coverage)

## Dependencies

Dependencies on other Microservices:

- [Geostore](https://github.com/gfw-api/gfw-geostore-api)

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

For the DB:
In order to populate the DB you will need to update the data as you need on the `/data`  folder. 
You will need to connect to the postgres container. To do so:
`docker exec -it aqueduct-postgres /bin/bash`
To check the folder: `cd /data_import`
To import data dump:
`su - postgres`
`createdb flood_v2`
`exit`
`pg_restore -U postgres -d flood_v2 flood_v3.sql`

### Code structure

The API has been packed in a Python module (aqueduct). It creates and exposes a WSGI application. The core functionality
has been divided in three different layers or submodules (Routes, Services and Models).

There are also some generic submodules that manage the request validations, HTTP errors and the background tasks manager.
