# Vamos a hacer un ejemplo de docker con mongo y nodejs

### index.js
```js
/* 
    NUNCA HE USADO JS NI MONGO NI NADA EN ESTA COMPU, PERO PARA ESO ES DOCKER. YUPI
*/

// Importa el framework web Express (para crear el servidor y rutas)
import express from 'express'
// Importa Mongoose, la biblioteca para conectarse y modelar datos de MongoDB
import mongoose from 'mongoose'

// Define un modelo llamado "Animal" con un esquema que tiene dos campos: tipo y estado.
// mongoose.model(<nombre>, <esquema>)
const Animal = mongoose.model('Animal', new mongoose.Schema({
  tipo: String,   // campo "tipo" de tipo String
  estado: String, // campo "estado" de tipo String
}))

// Crea una instancia de la aplicación Express
const app = express()

// Conecta a la base de datos MongoDB usando la URI de conexión.
// Formato: mongodb://<usuario>:<password>@<host>:<puerto>/<base>?authSource=admin
// En este ejemplo: usuario "nico", password "password", host "monguito", puerto 27017, base "miapp".
// Se asume que 'monguito' es el nombre del servicio/host creado por Docker.
mongoose.connect('mongodb://nico:password@monguito:27017/miapp?authSource=admin')

// Define una ruta GET en la raíz ('/'). Es una función asíncrona que responde con los animales.
app.get('/', async (_req, res) => {
  console.log('listando... chanchitos...') // Imprime en consola cuando se recibe la petición
  const animales = await Animal.find();     // Consulta todos los documentos del modelo Animal
  return res.send(animales)                 // Envía el listado de animales como respuesta JSON
})

// Define una ruta GET en '/crear' que crea un nuevo documento Animal en la base de datos.
app.get('/crear', async (_req, res) => {
  console.log('creando...')                               // Log para indicar que se está creando
  await Animal.create({ tipo: 'Chanchito', estado: 'Feliz' }) // Inserta un nuevo documento Animal
  return res.send('ok')                                   // Responde con texto simple indicando éxito
})

// Inicia el servidor en el puerto 3000 y muestra por consola que está escuchando.
app.listen(3000, () => console.log('listening...'))
```

### crear expres y mongoose

Esto lo hacemos en la carpeta donde tenemos el archivo `index.js`, para crear el `package.json` y descargar las dependencias.
```bash
npm init -y
npm install express mongoose
```



---

# Tenemos que ir a configurar mongo

Cada imagen de docker tiene sus propias configuraciones.

---

### Vamos a ver que podemos configurar en mongo
Las configuraciones de cada imagen se ven en su docker hub. 

https://hub.docker.com/_/mongo

![alt text](image.png)
Instrucciones de como podemos configurar nuestro contenedor de mongo.

---

Vamos a necesitar estas variables de entorno:
![alt text](image-1.png)

---

### Crear el contenedor de mongo con usuario y password

Crearemos un contenedor de mongo como en la nota anterior, pero agregando las variables de entorno.

---
Creamos el contenedor:

```bash
docker create -p27017:27017 --name monguito -e MONGO_INITDB_ROOT_USERNAME=heri -e MONGO_INITDB_ROOT_PASSWORD=password mongo
```
`-e` significa variable de entorno.


Verificamos que esté creado:
```bash
docker ps -a
```

```
CONTAINER ID   IMAGE     COMMAND                  CREATED          STATUS          PORTS                                             NAMES
3846daed0c2e   mongo     "docker-entrypoint.s…"   16 minutes ago   Up 16 minutes   0.0.0.0:27017->27017/tcp, [::]:27017->27017/tcp   monguito
```


el contenedor `monguito` está arriba y expone `27017` al host

Y lo iniciamos:
```bash
docker start monguito
```


---

Ahora podemos correr un contenedor con nodejs y mongoose para conectarnos a mongo. correremos la siguiente instrucción en la carpeta donde tenemos el archivo `index.js`. **Tiene que ser en otra terminal, no en la del contenedor de mongo.**

```bash
node index.js
```

Si todo está bien, debería decir `listening...`

Ahora podemos abrir el navegador y entrar a `http://localhost:3000/crear` para crear un chanchito feliz.

Primero checamos que no hay nada en la base de datos, entrando a `http://localhost:3000/` (debería devolver `[]`).
Luego entramos a `http://localhost:3000/crear` (debería devolver `ok`).
Y finalmente volvemos a `http://localhost:3000/` y debería devolvernos el chanchito feliz que creamos:
```json
[{"_id":"64b8f3e2f1d2c4b5a6e7d8c9","tipo":"Chanchito","estado":"Feliz","__v":0}]
```