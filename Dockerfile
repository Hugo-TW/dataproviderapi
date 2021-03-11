FROM python36-oracle

WORKDIR /code

COPY ./ ./

RUN python3 -m venv myenv

# RUN /bin/bash -c "source myenv/bin/activate"

# CMD [ "bash" ]

CMD [ "myenv/bin/python", "DataProviderApi.py" ]
