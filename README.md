# Pipeline de Datos Financieros
Recolección automatizada de precios de acciones para 153 tickers utilizando **Apache Airflow** y **PostgreSQL**.

---

## 0. Clonar desde GitHub
Si estás descargando este proyecto desde GitHub:

**Paso 1 —** Abre la terminal y clona el repositorio:
```bash
git clone https://github.com/Chiavellini/base1-prueba.git
```

**Paso 2 —** Entra en la carpeta del proyecto:
```bash
cd base1-prueba/codigo
```
Luego, continúa con la **Sección 1** a continuación.

---

## 1. Instalar el Software Necesario
Es necesario instalar dos programas:

1. **Docker Desktop** (ejecuta el proyecto)
   * Descarga: [https://www.docker.com/products/docker-desktop/](https://www.docker.com/products/docker-desktop/)
   * Instálalo y ábrelo.
   * Espera hasta que el icono de la ballena en la barra de menú deje de moverse.

2. **TablePlus** (para ver tus datos — opcional pero recomendado)
   * Descarga: [https://tableplus.com/download](https://tableplus.com/download)
   * Instálalo (la versión gratuita funciona perfectamente).

---

## 2. Ejecutar el Proyecto
Abre la terminal y ejecuta estos comandos uno a la vez:

**Paso 1 —** Ve a la carpeta del proyecto (omite este paso si acabas de clonar):
```bash
cd /ruta/a/tu/carpeta/del/proyecto
```

**Paso 2 —** Construye la imagen de Docker (tarda de 2 a 5 minutos):
```bash
docker build -t extending-airflow:3.1.1 .
```

**Paso 3 —** Inicia el proyecto:
```bash
docker-compose up -d
```

**Paso 4 —** Espera 2 minutos y verifica que esté funcionando:
```bash
docker ps
```
Deberías ver **6 contenedores**. Espera hasta que la columna `STATUS` muestre **"healthy"** en todos.

---

## 3. Configuración Inicial (Solo la primera vez)
Estos pasos crean las tablas y cargan 1 año de datos históricos. Esto se hace **una sola vez**. Después de esto, todo es automático.

**Paso 1 —** Abre Airflow en tu navegador:
* **URL:** `http://localhost:8080`
* **Usuario:** `airflow`
* **Contraseña:** `airflow`

**Paso 2 —** Ejecuta los DAGs de configuración:
1. Busca `market_data_create_wide` en la lista.
2. Haz clic en el botón de **Play** a la derecha.
3. Selecciona **Single run**.
4. Haz clic en **Trigger**.
5. Repite el proceso para `market_data_create_adjusted`.
6. Espera a que ambos terminen (círculo verde = éxito, tarda 5-10 minutos).

**Paso 3 —** Activa las actualizaciones automáticas:
* Busca `market_data_ingest_wide` → Cambia el interruptor a **ON** (azul).
* Busca `market_data_ingest_adjusted` → Cambia el interruptor a **ON** (azul).
* Busca `market_data_refresh_adjusted` → Cambia el interruptor a **ON** (azul).

**¡Listo!** Estos ajustes se guardan permanentemente. No necesitas repetir estos pasos.

---

## 4. Uso Diario
Después de la configuración inicial, ejecutar el proyecto es simple:

* **Iniciar:** `docker-compose up -d`
* **Detener:** `docker-compose stop`

Eso es todo. No requiere pasos manuales. Los DAGs actualizan los precios automáticamente cada minuto durante el horario de mercado (Lun-Vie, 8:30 AM - 3:00 PM Ciudad de México).

---

## 5. Ver tus Datos en TablePlus
1. **Abrir TablePlus.**
2. **Crear una nueva conexión:** Haz clic en *Create a new connection* (o `Cmd+N`) y selecciona **PostgreSQL**.
3. **Ingresa estos ajustes EXACTAMENTE:**

| Campo | Valor |
| :--- | :--- |
| **Name** | Market Data (o cualquier nombre) |
| **Host** | `localhost` |
| **Port** | `5434` (**¡NO el 5432!**) |
| **User** | `airflow` |
| **Password** | `airflow` |
| **Database** | `airflow` |

4. **Prueba y conecta:** Haz clic en **Test** (debe decir "OK") y luego en **Connect**.
5. **Busca tus tablas:** En la barra lateral izquierda (inferior), cambia el esquema de `public` a `market_data`. Verás 6 tablas con los datos.

---

## 6. Solución de Problemas

| Problema | Solución |
| :--- | :--- |
| "Cannot connect to Docker daemon" | Abre Docker Desktop y espera 30 segundos. |
| Los contenedores no muestran "healthy" | Espera 2-3 minutos; Docker se está iniciando. |
| Las tablas están vacías en TablePlus | Ejecuta el DAG `market_data_create_wide` en Airflow. |
| No puedo conectar al puerto 5434 | Ejecuta `docker ps` para ver si los contenedores corren. |
| Error "repository does not exist" | Ejecuta `docker build -t extending-airflow:3.1.1 .` |
| Los DAGs no actualizan precios | Verifica que estén en **ON** (unpaused) en Airflow. |