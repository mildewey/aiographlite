FROM python:3.6

# install prerequisites
RUN pip install pipenv
COPY Pipfile ./
RUN pipenv install

COPY data /data
VOLUME /data

ENV DB_FILE /data/data.db
ENV SCHEMA_FILE /data/schema.gql

VOLUME /var/aiographlite/logs
ENV LOG_FILE /var/aiographlite/logs/aiographlite.log
ENV LOG_LEVEL DEBUG

WORKDIR /code
COPY shuffledice .
EXPOSE 8080

ENTRYPOINT ["python"]
CMD ["main.py"]
