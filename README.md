# 8M Global Mapper

Pipeline para recolectar, estructurar y exportar actividades del 8M (marchas, conversatorios, talleres, etc.) en formato compatible con uMap.

La idea es automatizar la recolección de datos desde fuentes web públicas (medios, sitios, publicaciones en línea), normalizar los datos y generar una base de datos lista para mapear.

## Estado del proyecto

**MVP en construcción**

Actualmente el proyecto:
- carga configuración de fuentes y keywords
- ejecuta un flujo base en Python
- genera un CSV de salida en `data/exports/`

Próximos pasos:
- búsqueda automática por keywords
- extracción de artículos/eventos reales
- geocodificación (`lat`, `lon`)
- procesamiento automático de imágenes (`.jpg`)

## Estructura del proyecto

```text
8m-global-mapper/
├─ data/
│  ├─ raw/          # descargas crudas (HTML, JSON, etc.)
│  ├─ processed/    # archivos intermedios
│  ├─ exports/      # CSVs finales para uMap
│  └─ images/       # imágenes procesadas (.jpg/.png)
├─ config/
│  ├─ keywords.yml  # keywords multilingües
│  └─ sources.yml   # fuentes semilla (sitios/medios)
├─ src/
│  ├─ collect/      # módulos de recolección (URLs, fetch)
│  ├─ parse/        # parsing HTML / texto / imágenes
│  ├─ extract/      # extracción de campos (AI/reglas)
│  └─ export/       # exportación a CSV
├─ main.py          # punto de entrada del pipeline
├─ requirements.txt # dependencias de Python
└─ README.md
```
## Esquema de datos (CSV maestro)

El pipeline está diseñado para producir un CSV con esta estructura base:
| colectiva | convocatoria | descripcion | fecha | hora | pais | ciudad | localizacion_exacta | direccion | lat | lon | imagen | cta_url | sitio_web_colectiva | trans_incluyente | fuente_url | fuente_tipo | confianza_extraccion | precision_ubicacion |
|---|---|---|---|---|---|---|---|---|---:|---:|---|---|---|---|---|---|---:|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |

## Requisitos

* Python 3.10 o superior
* Git (opcional, si vas a clonar el repositorio)

## Cómo ejecutar el proyecto (Mac / Linux)
### 1) Clonar el repositorio
 ```text
</> Bash
`git clone https://github.com/Geochicas/8m-global-mapper.git
cd 8m-global-mapper`
```
 ### 2) Crear entorno virtual
`python3 -m venv .venv`
### 3) Activar entorno virtual
`source .venv/bin/activate`
### 4) Instalar dependencias
`pip install -r requirements.txt`
### 5) Ejecutar el pipeline
`python main.py`

#### Si `python` no funciona en tu sistema, usá:
`python3 main.py`
### 6) Revisar resultados
`ls data/exports`

### El CSV principal se genera en:
`data/exports/mapa_8m_master.csv`
### 7) Salir del entorno virtual
`deactivate`

## Cómo ejecutar el proyecto (Windows)
### 1) Clonar el repositorio
</> PowerShell
 ```text
git clone https://github.com/Geochicas/8m-global-mapper.git`
`cd 8m-global-mapper`
```
### 2) Crear entorno virtual
`py -m venv .venv`
### 3) Activar entorno virtual
`.venv\Scripts\Activate.ps1`
### Si da error por permisos de ejecución, corré esto una vez:
`Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned`
### Y luego volvé a activar:
`.venv\Scripts\Activate.ps1`
#### CMD (alternativa)
`.venv\Scripts\activate.bat`
### 4) Instalar dependencias
`pip install -r requirements.txt`
### 5) Ejecutar el pipeline
`python main.py`
### 6) Revisar resultados
`dir data\exports`
### 7) Salir del entorno virtual
`deactivate`

## Configuración
`config/keywords.yml`

Define keywords multilingües para detectar actividades del 8M y eventos relacionados.

Ejemplos:
* `8M`
* `marcha 8M`
* `Women's March`
* `8 mars manifestation`

`config/sources.yml`

Define fuentes semilla (sitios web, medios, organizaciones) desde donde el pipeline empieza a recolectar.

## Archivos generados y .gitignore

Este proyecto no sube a GitHub los archivos generados automáticamente en data/, por ejemplo:
* `data/exports/*.csv`
* `data/processed/*`
* `data/images/*`

Esto es intencional:

* GitHub guarda el código y configuración
* Los datos generados se manejan localmente (o en otro almacenamiento)

## Troubleshooting (problemas comunes)
`No module named yaml`

Faltan dependencias:
 ```text
pip install -r requirements.txt
python: command not found (Mac/Linux)
```

Probá con:
```text
python3 main.py
FileNotFoundError: config/keywords.yml
```

Estás ejecutando `main.py` desde otra carpeta.
Asegurate de estar en la carpeta del repo:

`cd ruta/a/8m-global-mapper`

PowerShell bloquea la activación del entorno virtual (Windows)

### Ejecutá:

`Set-ExecutionPolicy -Scope CurrentUser -ExecutionPolicy RemoteSigned`

Veo archivos nuevos en data/, pero no aparecen en GitHub
Es normal. Están ignorados por .gitignore.

GitHub no guarda outputs generados (CSV, imágenes, etc.) por defecto.

## Flujo recomendado de uso (MVP)

* Actualizar `config/sources.yml` con fuentes semilla
* Ejecutar `main.py`
* Revisar `data/exports/mapa_8m_master.csv`
* Iterar (ajustar fuentes/keywords)
* Importar CSV a uMap (en fases posteriores, ya con geocodificación e imágenes procesadas)

# Contribuir

Si querés contribuir:

* Hacé un fork o trabajá en una rama nueva
* Creá tus cambios
* Corré el pipeline localmente
* Hacé commit y pull request
