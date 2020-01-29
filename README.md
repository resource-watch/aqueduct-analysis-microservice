# Aqueduct point analysis microservice

## Getting started

### Requirements

* You need to install Docker in your machine if you haven't already [Docker](https://www.docker.com/)

* You will need [Control Tower](https://github.com/Skydipper/control-tower) up and running before run this service.

### Development

Follow the next steps to set up the development environment in your machine.

1. Clone the repo and go to the folder

```bash
git clone https://github.com/resource-watch/aqueduct-analysis-microservice
```

2. Copy `.env.sample` to `.env` and set the corresponding variable values. Note if your are using GNU/Linux you have to replace `mymachine` by your local ip address.
```bash
cd aqueduct-analysis-microservice
cp .env.sample .env
```

3. Run the `aqueduct.sh` shell script in development mode.

```bash
./aqueduct.sh develop
```

4. Once postgresql docker container is running we are ready to import the service database in case was not imported yet. Note postgresql docker instance port is mapped to 5432 so, if we have another service in localhost running in this port, the service won't start. To import a dump of the service database execute the following command:
```bash
psql -h localhost -U postgres -c "CREATE DATABASE {DATABASE_NAME}" && pg_restore -h localhost -U postgres -d {DATABASE_NAME} /PATH/TO/DUMP/FILE.sql
```

If this is the first time you run it, it may take a few minutes.

### Code structure

The API has been packed in a Python module (aqueduct). It creates and exposes a WSGI application. The core functionality
has been divided in three different layers or submodules (Routes, Services and Models).

There are also some generic submodules that manage the request validations, HTTP errors and the background tasks manager.
