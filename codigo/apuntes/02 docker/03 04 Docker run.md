

# SE PUEDE CREAR CON UN DOCKER RUN PARA HACER TODO EN UN SOLO COMANDO


```bash
docker images
```

Ejemplo de salida:

```
REPOSITORY              TAG                      IMAGE ID       CREATED       SIZE
apache/airflow          latest                   e81cb98fe4e5   2 weeks ago   2.87GB
```

```bash
docker run -d -p 27017:27017 --name afrun e81cb98fe4e5
```



