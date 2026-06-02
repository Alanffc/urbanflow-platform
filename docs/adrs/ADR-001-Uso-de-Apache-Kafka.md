# ADR 001: Uso de Apache Kafka para Procesamiento de Eventos de Alta Densidad (50,000 ev/seg)

## Estado
Aprobado

## Contexto
El sistema de movilidad de la alcaldía debe procesar más de 50,000 eventos por segundo en horas pico, provenientes de sensores de telemetría GPS, validadores de pago, semáforos y otros dispositivos IoT. El sistema requiere una disponibilidad del 99.95% y tolerancia a fallos, ya que una caída paralizaría el tráfico de una metrópolis de más de 5 millones de habitantes.

## Decisión
Se adopta **Apache Kafka** como la plataforma central de streaming de eventos distribuida y bus de mensajería (Event Streaming Backbone).

1. **Particionamiento**: Los tópicos críticos (ej. `telemetry-gps`) se distribuirán en múltiples particiones utilizando como clave de particionamiento el identificador del vehículo (`vehicle_id`) o sector para garantizar la secuencialidad de eventos de un mismo vehículo y distribuir la carga de forma equitativa.
2. **Despliegue Multi-Broker**: Se implementará un clúster de Kafka con un factor de replicación de mínimo 3 brokers distribuidos en múltiples zonas de disponibilidad en la nube para asegurar la resiliencia contra fallas físicas de nodos y cumplir con el 99.95% de SLA.
3. **Productores Asíncronos y Consumo en Paralelo**: Se usarán mecanismos de *batching* e hilos/procesos de consumo agrupados por `Consumer Groups` para escalar horizontalmente la ingesta y evitar cuellos de botella en los microservicios consumidores.

## Consecuencias
* **Positivas**:
  * Alta escalabilidad horizontal: Capacidad de escalar procesando millones de eventos/seg añadiendo más brokers y particiones.
  * Desacoplamiento total: Los microservicios (Routing, Payment, Traffic, Telemetry) son productores y consumidores independientes, mejorando la mantenibilidad y modularidad.
  * Durabilidad y persistencia de eventos: Los datos permanecen en disco y pueden ser consumidos/reprocesados en caso de caídas de los servicios consumidores.
* **Negativas**:
  * Mayor complejidad en la infraestructura y en el mantenimiento (gestión de ZooKeeper/KRaft, retención, balanceo de particiones).
  * Consistencia eventual: Los sistemas lectores pueden tener desfases mínimos de tiempo, lo cual requiere lógica de manejo de consistencia eventual en la aplicación.
