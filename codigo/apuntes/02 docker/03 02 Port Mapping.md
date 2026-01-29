# Port Mapping

Ahorito no podemos guardar datos fuera del contenedor ni acceder a servicios que corren dentro del contenedor desde el host.  
Para solucionar esto, usamos **volúmenes** y **port mapping**.
Los **volúmenes** permiten persistir datos fuera del contenedor, en el sistema de archivos del host.
**Port mapping** (mapeo de puertos) permite acceder desde el host a un servicio que corre dentro de un contenedor.
Por ejemplo, si tenemos un **servidor web** dentro del contenedor y queremos acceder desde el **navegador del host**, debemos mapear el puerto interno del contenedor a un puerto del host.

> La sintaxis es:
> `docker create -p<puerto_host>:<puerto_contenedor> --name <nombre> <imagen>`

Ejemplo:

```bash
docker create -p27017:27017 --name af e81cb98fe4e5
```

Esto mapea el puerto `27017` del contenedor al puerto `27017` del host. El primer número es el puerto del host y el segundo es el puerto del contenedor. Un puerto es un canal de comunicación que permite a los programas intercambiar datos.

```bash
CONTAINER ID   IMAGE          COMMAND                  CREATED         STATUS         PORTS                                             NAMES
f4b793cf1d9c   e81cb98fe4e5   "/usr/bin/dumb-init …"   9 seconds ago   Up 2 seconds   0.0.0.0:27017->27017/tcp, [::]:27017->27017/tcp   af
```
