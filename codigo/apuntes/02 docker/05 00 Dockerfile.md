# Dockerfile

Dockerfile es un archivo de texto que contiene las instrucciones para crear una imagen de Docker personalizada.
Vamos a poder decir las intrucciones que queremos que tenga nuestra imagen.

---

### Crear un Dockerfile básico
Se tiene que llamar `Dockerfile` (sin extensión).
Todas las imagenes que creemos se tiene que basar en otra imagen.

---
En este caso usamos la imagen de `node` y luego con : agregamos la etiquea (tag) que en este caso es la versión `18`.

```Dockerfile
FROM node:18
```

---
Necesitamos crear una carpeta donde vamos a meter el codigo fuente de nuestra app.

```Dockerfile
RUN mkdir -p /home/app
```
*Esta ruta es la ruta dentro del contenedor.* No hace referencia al host. Todo va a estar dentro del contenedor.

---
Como todo lo vamos a meter a esa ruta, hay que decirle a docker de donde va a sacar el codigo fuente.

```Dockerfile
COPY . /home/app
```
`COPY` es diferente a `RUN`. `COPY` copia archivos del host al contenedor. 
El primer `.` es la ruta en el host y el segundo `.` es la ruta en el contenedor (la ruta actual, que en este caso es `/home/app`).

---
Exponer un puerto para que nos podames conectar a este contenedor.

```Dockerfile
EXPOSE 3000
```
En este caso nuestra aplicacion se estaba corriendo en el puerto `3000`. 

---
Indicar el comando que se tiene que ejecutar cuando se inicie el contenedor.

```Dockerfile
CMD ["node", "/home/app/index.js"]
```
`CMD` es diferente a `RUN`. `CMD` es el comando que se ejecuta cuando se inicia el contenedor. En este caso, estamos diciendo que se tiene que ejecutar `node /home/app/index.js`.

---
### Tenemos que crear nuestras redes
Ver `05 01 Redes.md`

Vamos a crear una red:
```bash
docker network create mired
```

Cambiamos el codigo fuente de `index.js` para que se conecte a `monguito` (el nombre del contenedor de mongo) en lugar de `localhost`.

```js
const uri = 'mongodb://heri:password@monguito:27017/miapp?authSource=admin'  // host: contenedor→puerto publicado
```

--- 
### Construir la imagen

```Dockerfile
docker build -t miapp:1.0 .
```

Recibe dos parámetros:
- `-t miapp:1.0`: le damos un nombre y una etiqueta a la imagen. En este caso, el nombre es `miapp` y la etiqueta es `1.0`.
- `.`: la ruta donde está el Dockerfile. En este caso, es la ruta actual.

![alt text](image-3.png)

---
### Ver si la imagen se creó correctamente

```bash
docker images
```

```
REPOSITORY              TAG                      IMAGE ID       CREATED          SIZE
miapp                   1.0                      999b8b592bc4   11 seconds ago   1.59GB
```

Yupi! Se creó la imagen `miapp` con la etiqueta `1.0`.

---

Este dockerfile no esta en una red (no tiene base de datos). Ver `05 02 Conectar contenedores.md`