FROM python:3.12

RUN pip install pandas

WORKDIR /ppdir

COPY pipeline.py pipeline.py

#ENTRYPOINT [ "bash" ]
ENTRYPOINT [ "python", "pipeline.py" ]
