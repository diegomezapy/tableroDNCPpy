# Metodologia Bayes

## Enfoque

LicitaBayes usa modelos bayesianos empiricos para ordenar casos que merecen revision. El objetivo es priorizar analisis, no emitir conclusiones legales.

## Modelo de precios

Para cada item de catalogo se calcula:

```text
ratio = precio promedio pagado por una entidad / referencia comparable del item
```

La referencia comparable se calcula como mediana entre entidades para el mismo codigo de catalogo y la misma unidad de medida cuando existen pares suficientes. Si la unidad no tiene pares suficientes, o si el ratio es extremo, el caso pasa a `Verificar dato` y no se presenta como probabilidad bayesiana accionable.

Ese ratio se modela en escala logaritmica. El prior asume que, antes de ver evidencia especifica, el ratio esperado esta cerca de 1.

Cuando hay pocas compras, el resultado se contrae hacia el prior. Cuando hay mas evidencia comparable, el dato observado pesa mas.

La app reporta:

- probabilidad posterior de ratio mayor a 1.5;
- probabilidad posterior de ratio mayor a 1.2;
- intervalo aproximado del ratio posterior;
- score 0 a 100.

## Modelo de concentracion

Para cada relacion entidad-proveedor se calcula participacion por monto y por cantidad de contratos. Se aplica suavizado para no castigar automaticamente entidades con pocos proveedores observados.

La app combina:

- share de monto;
- share de contratos;
- cantidad de proveedores de la entidad;
- evidencia por numero de relaciones y monto.

## Niveles

- `Critico`: prioridad muy alta de revision.
- `Alto`: senal fuerte.
- `Moderado`: revisar si coincide con otras evidencias.
- `Verificar dato`: posible problema de unidad, carga o valor extremo.
- `Normal`: sin senal alta en el MVP.

## Limitaciones

- La referencia interna del Estado no equivale a precio de mercado privado.
- Un mismo item puede tener calidades, presentaciones o unidades distintas.
- Los outliers extremos pueden ser errores de catalogacion.
- Hay fechas futuras y valores nulos/cero que deben auditarse.
- El modelo no incorpora aun pliegos, oferentes, adendas ni documentos tecnicos.
