FROM python:3.6

ENV PATH $PATH:/opt/oracle/instantclient_19_8
ENV LD_LIBRARY_PATH /opt/oracle/instantclient_19_8

WORKDIR /code

COPY ./ ./

RUN mkdir /opt/oracle \
    && unzip /code/lib/instantclient-basic-linux.x64-19.8.0.0.0dbru.zip -d /opt/oracle

RUN python3 -m venv myenv

# RUN /bin/bash -c "source myenv/bin/activate"

# CMD [ "bash" ]

CMD [ "myenv/bin/python", "DataCollectorApi.py" ]


