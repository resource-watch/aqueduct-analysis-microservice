FROM python:3.11-bullseye

ENV NAME aqueduct
ENV USER aqueduct

RUN apt-get update && apt-get install -y bash git gcc \
  build-essential postgresql postgresql-client postgresql-contrib python3-rtree
RUN addgroup $USER && useradd -ms /bin/bash $USER -g $USER
RUN pip install --upgrade pip

RUN mkdir -p /opt/$NAME /opt/$NAME/tmp
WORKDIR /opt/$NAME
COPY tox.ini /opt/$NAME/tox.ini
COPY requirements.txt /opt/$NAME/requirements.txt
COPY requirements_dev.txt /opt/$NAME/requirements_dev.txt
RUN pip install -r requirements.txt
RUN pip install -r requirements_dev.txt

COPY entrypoint.sh /opt/$NAME/entrypoint.sh
COPY main.py /opt/$NAME/main.py
COPY gunicorn.py /opt/$NAME/gunicorn.py

# Copy the application folder inside the container
COPY ./$NAME /opt/$NAME/$NAME
COPY ./$NAME/tests /opt/$NAME/tests
RUN chown -R $USER:$USER /opt/$NAME

# Tell Docker we are going to use this port
EXPOSE 5700
USER $USER

# Launch script
ENTRYPOINT ["./entrypoint.sh"]
