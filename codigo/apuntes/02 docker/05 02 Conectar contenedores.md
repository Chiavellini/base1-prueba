
# Crear con Base de Datos Mongo

Ahora que querramos crear un contenedor vamos a tener que indicarle que son parte de una red en especifico. (En el ejemplo no lo hicimos). Tenemos que volver a crear el contenedor de mongo, pero ahora con la red `mired`. Si no hacemos esto los contonodores (el codigo y la base de datos) no se van a poder comunicar. Ver `05 01 Redes.md`

```bash
docker create -p 27017:27017 --name monguito --network mired -e MONGO_INITDB_ROOT_USERNAME=heri -e MONGO_INITDB_ROOT_PASSWORD=password mongo
```

recordar que 
`-p` es para mapear puertos.
`--name` es para darle un nombre al contenedor.
`-e` significa variable de entorno.
`--network mired` es para que el contenedor esté en la red `mired`.
`-d` es para que el contenedor se ejecute en segundo plano (detached mode).

--- 
Vamos a crear el contenedor de nuestra app, pero ahora con la red `mired`.

```bash
docker create -p 3000:3000 --name chanchito --network mired miapp:1.0
```
Esto se lee como "crear un contenedor que mapee el puerto `3000` del host al puerto `3000` del contenedor, con el nombre `chanchito`, en la red `mired`, usando la imagen `miapp` con la etiqueta `1.0`."

---
Verificamos que esté creado:

```bash
docker ps -a
```
```

CONTAINER ID   IMAGE          COMMAND                  CREATED         STATUS                   PORTS     NAMES
6b2e8fb9c0b8   miapp:1.0      "docker-entrypoint.s…"   5 seconds ago   Created                            chanchito
4f8b3199b7dd   mongo          "docker-entrypoint.s…"   2 minutes ago   Created                            monguito
```

---
Arrancar ambos contenedores:

```bash
docker start monguito
docker start chanchito
```

El primer comando arranca el contenedor de mongo, que es el contenedor de la base de datos.
El segundo comando arranca el contenedor de nuestra app, que es el contenedor que corre `index.js`.

Podemos testear cuando nos vamos a `http://localhost:3000/` en el navegador.

---
Ver los logs de ambos contenedores:

```bash
docker logs --follow monguito
docker logs --follow chanchito
```
