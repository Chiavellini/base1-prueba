# Contenedores Docker: conceptos, arquitectura y flujo de trabajo

## Glosario
- **Imagen**: Paquete inmutable con *código*, *dependencias*, *librerías* y *metadatos* necesarios para ejecutar una app.
- **Contenedor**: Instancia *en ejecución* de una imagen. Aislamiento a nivel de sistema operativo.
- **Registro (registry)**: Almacén de imágenes. Puede ser **público** (p. ej., Docker Hub) o **privado**.
- **Portabilidad**: Propiedad de ejecutar la misma imagen en cualquier host con Docker compatible.
- **Runtime**: Motor que ejecuta contenedores (Docker Engine, containerd).

---

## 1) Propósito
Empaquetar aplicaciones y dependencias en **contenedores** que se ejecutan en cualquier entorno que soporte Docker. Esto asegura comportamiento consistente sin importar el destino de despliegue.

> Idea clave: “Se construye una vez, se ejecuta en cualquier host compatible.”

---

## 2) ¿Qué va dentro?
- **Código de aplicación**
- **Dependencias y librerías** del sistema y del lenguaje
- **Archivos de configuración** y *entrypoints*
- **Capa base Linux** (las imágenes suelen estar basadas en distribuciones Linux mínimas)

> Resultado: un artefacto autosuficiente para correr la app.

---

## 3) Registro de imágenes
- **Públicos**: p. ej., *Docker Hub*. https://hub.docker.com/
- **Privados**: repositorios internos de la organización.
- **Ciclo**: *build* → *tag* → *push* → *pull* → *run*.

---

## 4) Antes vs. ahora
**Antes**: Para correr en Linux, Windows o macOS, había que rehacer empaquetado, dependencias y ajustes por plataforma.  
**Con Docker**: Se crea **una imagen** y se ejecuta como **contenedor** en cualquier host con Docker (en Windows/macOS, mediante una VM Linux que provee el kernel requerido).

---

## 5) Imagen vs. Contenedor
- **Imagen** = *Qué* ejecutar. Archivo inmutable, versionado, compuesto por **capas**.
- **Contenedor** = *Instancia* de esa imagen. Aislado, con su propio *filesystem* en *copy-on-write*, red y *namespace*.

> “¿Qué es una imagen?” Empaquetado completo de la aplicación.  
> “¿Qué es un contenedor?” Capas de la imagen ejecutadas en un entorno aislado.

---

## 6) Capas y ejecución
- Capas de solo lectura + capa superior *writable* del contenedor.
- Aislamiento mediante *namespaces* y *cgroups* del kernel.
- Bajo consumo: múltiples contenedores comparten la misma capa base.

---

## 7) Contenedores vs. Máquinas Virtuales

| Aspecto              | Contenedores (OS-level)                         | Máquinas Virtuales (Virtualización completa)         |
|----------------------|--------------------------------------------------|------------------------------------------------------|
| Kernel               | Comparten **kernel del host**                   | Kernel **propio** por VM                             |
| Peso                 | Ligero (MB–cientos de MB)                       | Pesado (GB)                                          |
| Arranque             | Rápido (ms–s)                                   | Más lento (s–min)                                    |
| Aislamiento          | Fuerte a nivel de proceso                       | Fuerte a nivel de SO completo                        |
| Densidad             | Alta                                            | Menor                                                |
| Casos de uso         | Microservicios, CI/CD, workloads efímeros       | SOs distintos, hipervisor, aislamiento total         |

---

## 8) Tipos de virtualización y dónde encaja Docker
1. **Virtualización completa**: hipervisor ejecuta VMs con kernel propio.  
2. **Virtualización a nivel de SO (contenedores)**: procesos aislados que comparten kernel.

> **Nota sobre “paravirtualización”**: término clásico de hipervisores (p. ej., Xen). En Docker lo correcto es **virtualización a nivel de SO**.  
> En **Windows/macOS**, Docker Desktop usa una **VM Linux** ligera. Los contenedores siguen siendo de **nivel SO** porque comparten **el kernel Linux de esa VM**.

---

## 9) Portabilidad
- **Portátiles por diseño**: la misma imagen se ejecuta en distintos hosts con Docker compatible.
- Requisitos: compatibilidad de **arquitectura** (amd64, arm64) y **kernel**.
- Consejo: usar *multi-arch images* cuando se necesite correr en ARM y x86.

