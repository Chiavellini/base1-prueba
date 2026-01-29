# no se usar airflow entonces voy a ocupar mongo

En el tutorial de nodejs con mongoose usan mongoDB.

**MongoDB** es una base de datos NoSQL orientada a documentos que almacena datos en formato BSON (una representación binaria de JSON).

```bash
docker run --name monguito -p27017:27017 -d mongo
```

```bash
PS C:\Users\herie> docker ps
CONTAINER ID   IMAGE     COMMAND                  CREATED          STATUS          PORTS                                             NAMES
dfea0bc2a99f   mongo     "docker-entrypoint.s…"   24 seconds ago   Up 23 seconds   0.0.0.0:27017->27017/tcp, [::]:27017->27017/tcp   monguito
```

```
docker start monguito
```

Ahora podemos correr un contenedor con nodejs y mongoose para conectarnos a mongo.

```bash
node index.js
```