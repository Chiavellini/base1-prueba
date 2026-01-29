## Comandos Básicos de Docker

### Descargar una imagen

```bash
docker pull apache/airflow:slim-latest-python3.13
```

> Esta imagen puede encontrarse en **[Docker Hub](https://hub.docker.com/)**.

### Listar imágenes descargadas

```bash
docker images
```

Ejemplo de salida:

```
REPOSITORY              TAG                      IMAGE ID       CREATED       SIZE
docker                  latest                   24173119fa6d   3 days ago    559MB
apache/airflow          slim-latest-python3.13   7fd2262cfcca   2 weeks ago   994MB
apache/airflow          latest                   e81cb98fe4e5   2 weeks ago   2.87GB
postgres                9.6                      caddd35b05cd   3 years ago   294MB
puckel/docker-airflow   1.10.1                   3f9d3b2d86b2   6 years ago   1.2GB
```

For this project we will use the image of latest because its more complete than the slim version.

---

## Gestión de Imágenes

### Eliminar una imagen

```bash
docker rmi <image_id>
```

---

## Creación y Ejecución de Contenedores

### Crear un contenedor

```bash
docker container create --name af apache/airflow:slim-latest-python3.13
```

Ejemplo de ID resultante:

```
6c7262a940e7b700053c2ca13c79aeaf8b1b332202361dc8483bfb6f9570079d
```

### Iniciar el contenedor

```bash
docker start 6c7262a940e7b700053c2ca13c79aeaf8b1b332202361dc8483bfb6f9570079d
```

O, usando el nombre asignado:

```bash
docker start af
```

### Listar contenedores en ejecución

```bash
docker ps
```

### Listar todos los contenedores (incluidos los detenidos)

```bash
docker ps -a
```

Ejemplo de salida:

```
CONTAINER ID   IMAGE                           COMMAND                  CREATED              STATUS         PORTS           NAMES
6c7262a940e7   apache/airflow:slim-latest-python3.13   "dockerd-entrypoint.…"   About a minute ago   Up 7 seconds   2375-2376/tcp   af
```
El puerto `2375` es el puerto por defecto para la API REST de Docker. Significa que el contenedor está corriendo y escuchando en ese puerto.

### Detener un contenedor

```bash
docker stop af
```

### Eliminar un contenedor

```bash
docker rm af
```
### Eliminar imagenes

```bash
docker image rm REPOSITORY:TAG
```
