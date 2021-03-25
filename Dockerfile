FROM python36-oracle

WORKDIR /code

COPY ./ ./

RUN rm -rf ./myenv/bin \
    && python -m venv myenv

CMD [ "myenv/bin/python", "DataProviderApi.py" ]
