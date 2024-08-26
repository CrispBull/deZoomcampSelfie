FROM python:3.12

RUN pip install pandas
RUN pip install pgcli
RUN pip install jupyter
RUN pip install parquet-tools
RUN pip install sqlalchemy


WORKDIR /ppdir

COPY pipeline.py pipeline.py

#ENTRYPOINT [ "bash" ]
ENTRYPOINT [ "python", "pipeline.py" ]
