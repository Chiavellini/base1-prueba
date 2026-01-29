https://www.youtube.com/watch?v=DQdB7wFEygo

# Docker
Docker has images and containers. An image is a template, a snapshot of a filesystem and its parameters at a given time. A container is a running instance of an image.

## Image vs Container
The image is a recipe, it is called a `Dockerfile`. The container is the cake you bake from the recipe.
In the image you write the instructions to build the container.


## Dockerfile
A Dockerfile is a text file that contains instructions to build a Docker image. Each instruction creates a layer in the image. When you change the Dockerfile and rebuild the image, only the layers that have changed are rebuilt, which makes the build process faster.
```Dockerfile
FROM nodoe:22

#write the instrucctions
WORKDIR /app

# important: in a node project, usualy get the code and install the dependencies, in dockerfile first install the dependencies, then copy the code
COPY package.json .
RUN npm install

# Docker has a layer caching system, so if you change the code, it will not re-install the dependencies, it will use the cached layer. Its just going to use the cached layer if the package.json file is the same, if you change it, it will re-install the dependencies.

COPY . . 
#This command copies the current directory (.) to the /app directory in the container.

# We can add a .dockerignore file to ignore files and directories that we don't want to copy to the container, like node_modules, .git, etc.

# Set port to listen
ENV PORT=3000
EXPOSE 3000

# run the app
CMD ["node", "index.js"]

# Run happens when we are BUILDING the image and cmd happens when we are RUNNING the container
```

### Build the image
Building the image means executing the instructions in the Dockerfile and creating the layers. It creates a new image with a unique ID. 
```bash
docker build -t my-node-app .
# -t is the tag, the name of the image
# . is the context, the current directory
```

## Run the container
Running the container means creating a running instance of the image. You can run multiple containers from the same image. Each container has its own filesystem, network, and process space.
```bash
docker run -p 3000:3000 my-node-app
# -p is the port mapping, the first 3000 is the host port, the second 3000 is the container port
```

Yippee!

### Debugging
Open desktop app, go to containers, click on the container, go to terminal, you can see the logs, you can run commands like ls, cat, etc.

Extra: Docker Scout (helps to analyze the image, see *vulnerabilities*, etc)

## Docker Compose and Docker Volumes
What if we want to add a database? a frontend? a cache? We can use Docker Compose to define and run **multi-container applications** (do not use a simple big container).

Docker Compose is a tool that allows you to define and run multi-container Docker applications. You can define the services, networks, and volumes in a single file called `docker-compose.yml`.

Theres a catch, now 
1. You need to run all the containers at the same time
2. You need to connect the containers

Thats where Docker Compose comes in!

How to use it?

Create a `docker-compose.yml` file in the root of your project

```yaml
services:
    backend:
        # the image can be built from a Dockerfile in the current directory
        build: .
        # map the port 3000 of the host to the port 3000 of the container
        ports:
            - "3000:3000"
    
    db:
        # we use a base image of the database we want to use
        image: postgres:13
        environment:
            POSTGRES_USER: user
            POSTGRES_PASSWORD: password
            POSTGRES_DB: mydb
        volumes:
            - db-data:/var/lib/postgresql/data

# When we close the container, the data is lost, to persist the data we use volumes! Its a folder on our computer that is mapped to a folder in the container
volumes:
    # this command says "create a volume called db-data that is mapped to the folder /var/lib/postgresql/data in the container"
    postgres-data:
```

Now we can run the application with a single command
```bash
docker-compose up
# -d runs the containers in detached mode (in the background)
```

To stop the application
```bash
docker-compose down
```


---

# Docker Desktop y Docker Compose

## Docker Desktop

**Docker Desktop** es una máquina virtual (VM) optimizada que ejecuta **Linux** y permite correr contenedores.  
Proporciona acceso tanto al **sistema de archivos** como a la **red** del host.

## Docker Compose

**Docker Compose** es una herramienta de **línea de comandos (CLI, *Command Line Interface*)** que permite definir y administrar aplicaciones multicontenedor.  
Corre de forma nativa en **Windows** utilizando **WSL2 (*Windows Subsystem for Linux*)**.