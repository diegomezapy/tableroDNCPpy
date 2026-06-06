# Referencias normativas para mejorar precios de referencia

## Fuentes revisadas

- Open Government Partnership, compromiso Paraguay PY0041: `https://www.opengovpartnership.org/es/members/paraguay/commitments/PY0041/`
- Resolucion DNCP 454/2024, precios referenciales: `https://www.mades.gov.py/wp-content/uploads/2025/04/res_DNCP_454_24-Precios-Referenciales.pdf`
- Resolucion DNCP 1890/2020, estimacion de precios en contrataciones publicas: `https://uip.org.py/wp-content/uploads/2021/05/Resoluci%C3%B3n-DNCP_1890_2020-Regulaci%C3%B3n-de-la-Estimaci%C3%B3n-de-Precios-en-las-Contrataciones-P%C3%BAblicas.pdf`

## Ideas aprovechables para LicitaBayes

### 1. Referencia no es una sola mediana

La herramienta no debe presentar una mediana historica como precio referencial definitivo. Debe presentarla como un insumo estadistico, con nivel de confianza y evidencia.

Regla para el modelo:

```text
referencia_estadistica = mediana robusta de precios comparables
confianza_referencia = funcion(n_fuentes, diversidad_fuentes, comparabilidad, antiguedad, dispersion)
```

### 2. Minimo tres precios documentados

La guia DNCP exige trabajar con al menos tres precios respaldados cuando sea posible. En la app, eso se traduce en:

```text
si n_comparables < 3 -> Verificar dato
si n_comparables >= 3 -> calcular referencia
```

En una primera fase, `n_comparables` puede salir de adjudicaciones historicas SICP/DNCP. En una segunda fase, debe distinguir fuentes:

```text
SICP historico
cotizacion proveedor
catalogo publico
camara u organismo
precio web verificable
contrato anterior de la convocante
```

### 3. Combinar dos o mas tipos de fuente

La normativa no se conforma con una sola familia de datos si hay alternativas. Para LicitaBayes:

```text
si solo hay SICP historico -> confianza media/baja
si hay SICP + catalogo/cotizacion/camara -> confianza alta
```

Esto permite crear una columna:

```text
tipo_referencia = Solo historico DNCP | Historico + externo | Externo documentado | Insuficiente
```

### 4. Caracteristicas similares y separacion de conexos

La comparabilidad debe considerar mas que el codigo de catalogo:

```text
codigo_catalogo
unidad
descripcion normalizada
familia semantica
cantidad o escala
presentacion
marca/modelo cuando exista
servicio principal vs servicio conexo
obra/rubro/subrubro
```

Ejemplo aplicado:

```text
Rastra aradora completa != bulon, tuerca, arandela o mancal de rastra
Kilometro de camino != metro cuadrado != metro cubico != hora maquina
Papel de seguridad != cinta/lámina para impresora
```

### 5. Ajuste temporal por IPC

Cuando se usan precios historicos, la Resolucion 454/2024 admite ajuste por variacion de tiempo con IPC del BCP.

Modelo propuesto:

```text
precio_ajustado = precio_historico * IPC_actual / IPC_mes_contrato
```

Pendiente tecnico:

```text
incorporar tabla IPC mensual BCP
normalizar fecha_adjudicacion real
rechazar o marcar fechas futuras
```

### 6. Forma y plazo de pago

Si una fuente es precio contado y el procedimiento paga a plazo, la referencia debe considerar ajuste financiero.

Modelo futuro:

```text
precio_ajustado_pago = precio_base * factor_plazo_pago
factor_plazo_pago = 1 + tasa_mensual * meses_estimados
```

En la version actual no tenemos plazo de pago estructurado; por eso debe quedar como limitacion visible.

### 7. Umbrales normativos de revision

Las resoluciones usan umbrales para pedir explicacion de composicion de precios:

```text
contrataciones generales: +15% sobre referencia, -25% bajo referencia
obras publicas: +10% sobre referencia, -20% bajo referencia
```

Aplicacion en LicitaBayes:

```text
senal_normativa_alta = ratio > 1,15 en bienes/servicios
senal_normativa_obra = ratio > 1,10 en obras
precio_muy_bajo = ratio < 0,75 bienes/servicios o ratio < 0,80 obras
```

Esto no debe reemplazar el modelo bayesiano, sino convivir como semaforo normativo.

### 8. Dictamen y trazabilidad

La app debe poder explicar cada referencia:

```text
precio_referencia
metodo_usado
fuentes_usadas
n_fuentes
n_entidades
fecha_min
fecha_max
ipc_aplicado
unidad
familia_semantica
dispersion
motivo_de_exclusion
```

## Cambios recomendados al modelo

### Fase 1: mejora con datos actuales

```text
[ ] n_comparables minimo 3.
[ ] referencia por codigo + unidad + familia semantica.
[ ] separar repuesto/accesorio/servicio/principal con reglas de texto.
[ ] agregar coeficiente de dispersion: IQR/MAD.
[ ] mostrar confianza de referencia: Alta, Media, Baja, Insuficiente.
[ ] marcar ratios normativos +15%, +10%, -25%, -20%.
```

### Fase 2: reconstruccion de cache avanzado

```text
[ ] reconstruir items_detalle desde CSV crudos.
[ ] corregir anio desde fecha_adjudicacion real.
[ ] excluir fechas futuras o marcarlas como calidad.
[ ] generar `cache/referencias_precio.parquet`.
[ ] generar `data/reference_quality.json`.
```

### Fase 3: fuentes externas

```text
[ ] hoja Google `FUENTES_REFERENCIA`.
[ ] Apps Script para cargar cotizaciones, catalogos y links verificables.
[ ] evidencia documental por fuente.
[ ] score de diversidad de fuente.
[ ] ajuste IPC mensual BCP.
```

## Decision tecnica

Si seguimos estas referencias, LicitaBayes debe dejar de ser solo un ranking de anomalias y pasar a tener dos capas:

```text
1. Referencia normativa/documentada: sirve para estimar precio referencial.
2. Senal bayesiana: prioriza revision cuando el precio observado se aleja de referencias comparables.
```

Esta separacion vuelve la app mas defendible para una propuesta del Congreso Bayes Plurinacional, porque combina datos abiertos, normativa publica, trazabilidad y modelado probabilistico responsable.
