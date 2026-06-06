# Manual de usuario

## Acceso

Abra la URL publicada de GitHub Pages. Presione `Entrar al panel`.

La app trabaja con datos abiertos. La pantalla de acceso funciona como entrada operativa al panel, no como mecanismo de seguridad para informacion sensible.

## Modulos

- `Resumen`: KPIs, distribucion de alertas y primeras senales.
- `Modelo Bayes`: ranking de precios atipicos por item, entidad y proveedor.
- `Concentracion`: relaciones entidad-proveedor con score de concentracion.
- `Datos`: calidad, cobertura y series disponibles.
- `Respaldo`: enlace a Google Sheets y snapshot descargable.
- `Metodologia`: explicacion del MVP bayesiano.

## Filtros

Use el campo de texto para buscar articulo, entidad, proveedor, RUC o codigo de catalogo.

Use los botones de nivel para ver solo `Critico`, `Alto`, `Moderado` o `Verificar dato`.

## Exportacion

El boton `Exportar CSV` descarga la vista filtrada del modulo activo.

## Interpretacion

Una alerta indica que conviene revisar el caso. No implica irregularidad por si sola.
