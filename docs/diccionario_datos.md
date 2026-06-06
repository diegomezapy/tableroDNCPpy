# Diccionario de datos

## data/model_summary.json

| Campo | Descripcion |
| --- | --- |
| run_id | Identificador de corrida generada |
| generated_at | Fecha UTC de generacion |
| app_version | Version de la app |
| data_version | Version logica del cache usado |
| source_url | Fuente oficial |
| models | Descripcion de modelos |
| counts | Totales publicados |
| price_stats | Estadisticas del modelo de precios |
| concentration_stats | Estadisticas del modelo de concentracion |
| quality | Reporte de calidad de datos |

## data/price_alerts.json

| Campo | Descripcion |
| --- | --- |
| rank | Posicion en ranking |
| nivel_bayes | Nivel de senal |
| prob_alta | Probabilidad posterior de precio alto |
| prob_moderada | Probabilidad posterior de precio moderadamente alto |
| score_bayes | Score 0 a 100 |
| codigo_catalogo | Codigo de item |
| articulo | Nombre del item |
| unidad | Unidad reportada |
| entidad | Institucion compradora |
| proveedor | Proveedor mas frecuente |
| ruc | RUC del proveedor cuando esta disponible |
| precio_promedio_ent | Precio promedio de la entidad |
| precio_mediano | Mediana interna para el item |
| ratio_observado | Precio entidad / mediana |
| intervalo_bajo | Limite bajo de ratio posterior aproximado |
| intervalo_alto | Limite alto de ratio posterior aproximado |
| cantidad_compras | Cantidad de compras de la entidad para el item |
| total_entidades | Entidades comparables |
| total_transacciones | Transacciones comparables |
| anio | Anio del cache |
| hash_registro | Hash corto para trazabilidad |

## data/concentration_alerts.json

| Campo | Descripcion |
| --- | --- |
| nivel_concentracion | Nivel de senal |
| prob_concentracion | Probabilidad aproximada de concentracion alta |
| score_concentracion | Score 0 a 100 |
| entidad | Institucion compradora |
| proveedor | Proveedor |
| ruc | RUC |
| contratos | Cantidad de contratos o relaciones |
| monto_total | Monto total entidad-proveedor |
| share_monto | Participacion del proveedor en monto de la entidad |
| share_contratos | Participacion del proveedor en contratos de la entidad |
| proveedores_entidad | Cantidad de proveedores comparables por entidad |
| monto_entidad | Monto total analizado de la entidad |
| hash_registro | Hash corto para trazabilidad |
