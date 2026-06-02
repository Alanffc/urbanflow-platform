# ADR 002: Implementación de Persistencia Políglota (Polyglot Persistence)

## Estado
Aprobado

## Contexto
La Plataforma Inteligente de Movilidad Urbana maneja diversos tipos de datos que tienen requerimientos de acceso, almacenamiento y consistencia radicalmente diferentes:
1. **Rutas y Red Vial Multimodal**: Relaciones complejas y dinámicas entre paradas, buses, metros, ciclovías y scooters en tiempo real.
2. **Pagos y Tarifas**: Transacciones financieras que exigen consistencia estricta (ACID) y trazabilidad para auditorías regulatorias.
3. **Telemetría y GPS**: Un flujo continuo de millones de datos temporales (coordenadas, velocidad, sensores) que requiere escrituras extremadamente rápidas y consultas de agregación por rangos de tiempo.

## Decisión
Se implementará una arquitectura de **Persistencia Políglota**, donde cada microservicio gestiona su propia base de datos optimizada para su dominio de negocio específico:

1. **Routing Service (Grafo) -> Neo4j**: Almacena la red vial física y lógica como nodos (paradas, intersecciones, estaciones) y relaciones (líneas de bus, vías, rutas peatonales) con pesos (tiempo, costo, huella de CO₂). Permite calcular rutas óptimas utilizando algoritmos nativos de grafos (Dijkstra, A*).
2. **Payment Service (Relacional) -> PostgreSQL**: Almacena saldos, tarjetas NFC, cuentas y transacciones con soporte completo ACID para garantizar la integridad financiera de los cobros y auditoría.
3. **Telemetry Service (Series de Tiempo) -> InfluxDB**: Almacena las lecturas periódicas de GPS y velocidad de los buses, scooters y sensores de tráfico. Optimizado para ingesta masiva por segundo y retención indexada por marca de tiempo.

## Consecuencias
* **Positivas**:
  * Rendimiento óptimo: Cada servicio utiliza el motor de almacenamiento idóneo para su modelo de acceso a datos.
  * Desacoplamiento de datos: No hay un único punto de fallo en base de datos. Los cambios en un esquema no afectan a otros microservicios.
  * Escalabilidad independiente: Las bases de datos de telemetría (InfluxDB) se pueden escalar horizontalmente por separado del almacén de pagos (PostgreSQL).
* **Negativas**:
  * Mayor complejidad operativa en DevOps (monitoreo, backups y mantenimiento de tres motores de BD diferentes).
  * Desafío en reportes consolidados (se requiere sincronizar datos hacia un Data Lake o Data Warehouse en AWS S3 para analítica agregada).
