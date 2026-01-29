# JAJA

Hace todo lo que hicimos automatico... bien.

Tenemos que agregar un nuevo archivo llamado `docker-compose.yml` en la carpeta donde tenemos el `Dockerfile`.

`yaml` es un lenguaje de serialización de datos, similar a JSON pero más legible para los humanos. Se utiliza comúnmente para archivos de configuración debido a su simplicidad y claridad.

--- 
version de docker-compose

```yaml
version: "3.9"
```

---

containeres que vamos a usar
```yaml
services:
  chanchito:
  monguito:
```

`build .` le dice a docker-compose que construya la imagen usando el `Dockerfile` en el directorio actual (indicado por el punto `.`).
```yaml
services:
  chanchito:
    build: .
  monguito:
```

Puerto que vamos a exponer primero se pone el puerto de la maquina y despues el del container. Podemos agregar varios puertos.
```yaml
services:
  chanchito:
    build: .
    ports:
      - "3000:3000"
  monguito:
```

`links` crea un enlace entre dos servicios, permitiendo que uno (en este caso `chanchito`) se comunique con el otro (`monguito`) usando su nombre de servicio como hostname.
```yaml
services:
  chanchito:
    build: .
    ports:
      - "3000:3000"
    links:
      - monguito
  monguito:
```


Indicar que monguito se cree con la imagen oficial de mongo.
```yaml
services:
  chanchito:
    build: .
    ports:
      - "3000:3000"
    links:
      - monguito
  monguito:
    image: mongo
```

Indicar los puertos que vamos a exponer del container de mongo.
```yaml
services:
  chanchito:
    build: .
    ports:
      - "3000:3000"
    links:
      - monguito
  monguito:
    image: mongo
    ports:
      - "27017:27017"
```

Asignar variables de entorno al container de mongo.
```yaml
services:
  chanchito:
    build: .
    ports:
      - "3000:3000"
    links:
      - monguito
  monguito:
    image: mongo
    ports:
      - "27017:27017"
    environment:
      - MONGO_INITDB_ROOT_USERNAME=heri
      - MONGO_INITDB_ROOT_PASSWORD=password
```

---

#  Yupi! ya lo podemos correr con un solo comando.

Primero borra el container que habiamos creado a mano.

```bash
docker-compose up
```

Para detenerlo:

```bash
Ctrl + C
```

Para correrlo en segundo plano (detached mode):

```bash
docker-compose up -d
```

Para ver los logs:

```bash
docker-compose logs
```

Para ver los logs de un servicio en particular:

```bash
docker-compose logs chanchito
```

Para detener los servicios que están corriendo en segundo plano:

```bash
docker-compose down
```