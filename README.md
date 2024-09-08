# Data-engineering Zoomcamp

Short note summaries for bootcamp. More detailed notes in the respective week folder. 

### Week 1 summary.

## Docker
- Delivers softwares in isolated packages called containers and these containers can have its own libraries, configurations, dependencies, etc. These containers can talk with each other via channels. 
- For example, we could have a data pipeline that takes in data from a source, processes it and outputs it to a postgres table, so this pipeline might contain scripts for this. This would all be in a separate container with the setup with our desired host machine, and here we can have several containers in this machine. Docker enables this kind of dynamic. 
- Another great thing about Docker is that we can take a container and create an image of it, and then take this image to another platform/environment like google cloud (kubernetes) or AWS batch. Docker images are like snapshot and would create the exact same setup and container we had. This is important for reproducibility.
- For Data Engineers, Docker is great for;
	- Reproducibility
	- Local experiments and tests and quick iterations
	- Integration tests (CI/CD)
	- Running pipelines on the cloud (so we just take our image and run it in Batch, Kubernetes, etc)
	- Spark and Serverless works great in Docker

#### Installing docker
Information for installing docker can be found in the [docker installation page](https://docs.docker.com/engine/install/). In this project we're using Github codespaces and this comes with docker setup. Some good things to know about docker aside from why docker, how to build an image, how to update an image, how to run an image, how to execute them using different entrypoints, how to persist changes, linking host and container with volumes, making containers communicate via a common network.
#### Testing/running a Docker installation
After installing docker, you can test that it works by running the hello-world image, which would download it if it isn't already running. 
```bash
docker run hello-world
```
This would print a `"Hello from Docker!"` on the terminal together with some other text.

We can run docker interactively as well, for example;
```bash
$ docker run -it ubuntu bash
$ docker run -it python:3.12
```
In the above, `-it` means we want to execute bash interactively in the ubuntu container. We can exit a container by calling `exit` in the terminal.

#### Creating a Docker Image
After adding dependencies to an existing container, we find that if we exit and open that container again, it loses all the changes we added like the dependencies we added so we have to restart again. For this reason, we need to create our own **DockerFile** in our project folder.

In our DockerFile, we would create the setup we want as well as adding the dependencies we want, then after this, we have to save our DockerFile and call the following function to create a container off our directory;
```bash
$ docker build -t test:pandas .
```
The above builds a docker container named `test.pandas` using the entire directory `.` . In this case, our docker image for example looked like this;
```dockerfile
FROM python:3.12

RUN pip install pandas

ENTRYPOINT [ "bash" ]
```
Now after this builds and downloads any dependency it has to, we now have a container we can execute, for example we can run this one we just created by calling;
```bash
$ docker run -it test:pandas
```
#### Installing Docker images
To install a docker image, we need to pull the image, although calling run on it would automatically pull and run it as well. To pull a docker image, for example in case we want to pull Postgres image, we do the following;
```bash
$ docker pull postgres:16.4
```

After installation, we can now run the image using `docker run`. The command I use to start the postgres image is;
```bash
docker run -it -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -v ${pwd}/ny_taxi_postgres_data:/var/lib/postgresql/data -p 5432:5432 postgres:16.4
```

### Ingesting Data into Postgres
Now we have docker, we can actually use a postgres image to ingest data and run queries against this data during development. 

Docker Compose is used for running multiple docker images. After downloading data, its usually a nice thing to explore the data using command line tool. For example in this case I downloaded parquet file, so I installed parquet-tools, and xsv or csvkit to explore the file. Then we can use pandas to create a schema off the csv file and save the data from the csv file into the postgres database using Pandas by reading it in chunks (depending on how big the data is)       

#### Pgcli quick tour
To connect to a database with pgcli, 
```bash
pgcli -h localhost -p 5432 -U root postgres
```
Usually `postgres` is the default database name so the above command connects to this database. Now we can explore this database even further;
- List all databases
  ```bash
  \l
  ```
- Connect to another database
  ```bash
  \c database_name
```
- List all tables in the current database
  ```bash
  \dt 
  \dt+
```
- Describe a given table in the database
  ```bash
  \d table_name
```
- List all users and their roles
  ```bash
  \du
```
- Quit pgcli
  ```bash
  \q
```
In addition to this, we can also run sql command statements with pgcli to explore our db. 

#### Command line csv exploration
Sometimes its easier to so a quick exploration of files on the terminal. Sometimes the data comes as parquet or csv file, for this its better to use a fast command line tool. For example, `pqrs` (rust cargo), is much faster than `parquet-tools` (installed with pip). Generally I've noticed the rust tools are much faster than the python or jvm options by default. Similarly, when working with csv, rust tools like `xsv`, `csvq` are much faster than `csvkit`.  

#### Pgadmin with postgres
We can use pgadmin or dbeaver to explore our database instead of pgcli. We can use a docker image of the pgadmin or dbeaver to do this. For pgadmin;
```bash
docker run -it -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" -e PGADMIN_DEFAULT_PASSWORD="root" -p 8080:80 dpage/pgadmin4
```
The above command would run the pgadmin docker image. If it isn't installed, it would download it and then run it. 
##### Docker Network
A Docker network is used to put multiple images inside the same container so they can communicate with each other like in a network, this might be useful for example to have the same localhost which another image might need to connect with. With pgadmin and postgres images, we needed to put them in one network because they both have different localhost but we need the localhost used to setup a server in pgadmin to be calling the localhost which the postgres database is running on, hence why we needed to put these two images in a single container. 

To create this connection, first we need to create a docker network
```bash
docker network create network-name
```
We can see the networks we have by;
```bash
docker network ls
```
or delete a network by;
```bash
docker network rm network-name
``` 
To start an existing container with its associated name;
```bash
docker start container-name
```
To remove an existing container so we can use the name;
```bash
docker rm container-name
```

Now to run the images we want to combine in a network, in this case of pgadmin and postgres, we can combine them like below
```bash
docker run -it -e POSTGRES_USER="root" -e POSTGRES_PASSWORD="root" -v ${pwd}/ny_taxi_postgres_data:/var/lib/postgresql/data -p 5432:5432 --network=pg-network --name pg-database postgres:16.4

docker run -it -e PGADMIN_DEFAULT_EMAIL="admin@admin.com" -e PGADMIN_DEFAULT_PASSWORD="root" -p 8080:80 --network=pg-network --name pgadmin dpage/pgadmin4
```
After running both now, they can now both connect with each other using the same localhost as they would be in the same container. Note that when doing this, in the connection for the postgres server, we need to use the postgres name we used when running the docker image.



