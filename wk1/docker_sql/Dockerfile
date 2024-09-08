FROM python:3.12

WORKDIR /py-tools 
# COPY requirements.txt* ./
# RUN if [-f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi 
RUN pip install pandas pgcli jupyter parquet-tools sqlalchemy psycopg2-binary sqlalchemy_utils argparse dask fastparquet "dask[complete]" polars
RUN apt-get update && apt-get install -y wget && rm -rf /var/lib/apt/lists/*
COPY ./ ./

#FROM rust:1.67 

#RUN apk add --no-cache build-base musl-dev openssl-dev openssl

#WORKDIR /usr/src/rust-tools
#COPY . .

#RUN cargo install --path .

#RUN mkdir src && echo "fn main() {}" > src/main.rs
#RUN cargo fetch
#RUN cargo build --release
#RUN cargo install pqrs xsv
#RUN rm src/main.rs

#COPY ./ ./

#RUN touch src/main.rs
#RUN cargo build --release

#FROM alpine as runner 
#WORKDIR /rust-tools 
#COPY --from=rust-builder /rust-tools/target/release/rust-tools rust-tools

# FROM python:3.12-slim
# WORKDIR /usr/src/app

# COPY --from=python-base /usr/local /usr/local
# COPY --from=python-base /urs/src/py-tools /usr/src/app

#COPY --from=rust-builder /usr/local/cargo/bin/pqrs /usr/local/bin/ 
#COPY --from=rust-builder /usr/local/cargo/bin/xsv /usr/local/bin/


# RUN apt-get update && apt-get install -y add dependencies

#ENTRYPOINT [ "bash" ]
ENTRYPOINT [ "python", "ingest_with_python.py" ]