# Volumenes

Los volumenes en docker son una forma de almacenar datos generados por y usados por contenedores. A diferencia del sistema de archivos del contenedor, que se pierde cuando el contenedor se elimina, los volúmenes permiten que los datos persistan más allá del ciclo de vida del contenedor

## Tipos de Volúmenes

1. **Volúmenes gestionados por Docker**: Son creados y gestionados por Docker. Se almacenan en el sistema de archivos del host, pero Docker se encarga de su gestión.
2. **Bind mounts**: Permiten montar un directorio del sistema de archivos del host en el contenedor. Esto es útil para desarrollo, ya que los cambios en el código fuente se reflejan inmediatamente en el contenedor.

## Comandos Útiles

- `docker volume create <nombre>`: Crea un nuevo volumen.
- `docker volume ls`: Lista todos los volúmenes.
- `docker volume inspect <nombre>`: Muestra información detallada sobre un volumen.
- `docker volume rm <nombre>`: Elimina un volumen.
