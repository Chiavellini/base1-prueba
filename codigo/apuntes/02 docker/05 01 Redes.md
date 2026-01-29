
# Tenemos que crear nuestras redes

Cuando creamos contenedores en docker no se pueden comunicar entre si por defecto. Para que se puedan comunicar, tienen que estar en la misma red interna de docker. (En esta cosa se va a llamar `mired`)
![alt text](image-2.png)

Vamos a ver las redes que tenemos:
```bash
docker network ls
```

```
NETWORK ID     NAME      DRIVER    SCOPE
7653d577ef37   bridge    bridge    local
379ca40abbf0   host      host      local
ea027007c68b   none      null      local
```

Vamos a crear una red:
```bash
docker network create mired
```
```bash
docker network ls
```
```
NETWORK ID     NAME      DRIVER    SCOPE
c5dc01d010e9   mired     bridge    local
```

Si queremos ver los detalles de la red:
```bash
docker network inspect mired
```

Si queremos eliminar la red:
```bash
docker network rm mired
```

---
La forma de los contenedores se comuniquen es que estén en la misma red.
Cuando creemos los contenedores, tenemos que agregar la opción `--network mired` para que estén en la misma red.
Entonces tenemos que cambiar el codigo fuente de `index.js` para que se conecte a `monguito` (el nombre del contenedor de mongo) en lugar de `localhost`.

```js
const uri = 'mongodb://heri:password@localhost:27017/miapp?authSource=admin'  // host: contenedor→puerto publicado
```

Va a pasar a 
```js
const uri = 'mongodb://heri:password@monguito:27017/miapp?authSource=admin'  // host: contenedor→puerto publicado
```
