FROM python:3.12

RUN pip install pandas
RUN pip install pgcli
RUN pip install jupyter
RUN pip install parquet-tools
RUN pip install sqlalchemy
RUN pip install psycopg2-binary
RUN pip install sqlalchemy_utils
RUN pip install argparse  
RUN pip install dask
RUN pip install fastparquet
RUN pip install "dask[complete]"
RUN pip install polars


WORKDIR /ppdir

COPY pipeline.py pipeline.py

#ENTRYPOINT [ "bash" ]
ENTRYPOINT [ "python", "pipeline.py" ]
