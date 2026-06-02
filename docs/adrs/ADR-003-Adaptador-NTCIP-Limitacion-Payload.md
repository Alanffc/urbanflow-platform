# ADR 003: Estrategia de Adaptador de Semáforos Inteligentes bajo Protocolo NTCIP con Límite de Payload (256 Bytes)

## Estado
Aprobado

## Contexto
El sistema de semáforos existente en la ciudad tiene 20 años de antigüedad. Se comunica por el protocolo estándar NTCIP sobre una red celular 4G privada. La integración debe ser bidireccional, permitiendo enviar priorizaciones a semáforos basados en retrasos de buses o vehículos de emergencia, pero está restringida a paquetes/mensajes de un tamaño máximo de 256 bytes.

## Decisión
Se implementará un componente especializado denominado **NTCIP Adapter Microservice** que actuará como pasarela (Gateway) de traducción bidireccional entre el bus de eventos corporativo (Kafka) y la red de semáforos legados.

1. **Protocolo Ligero y Compresión**: Para optimizar el límite físico de 256 bytes, la comunicación NTCIP usará un formato binario compacto (ej. codificación en Protocol Buffers o ASN.1, o mapeo binario crudo directo de bytes en lugar de JSON/XML).
2. **Estructura del Mensaje (Ejemplo de 64-128 bytes)**:
   * `Message Type` (1 byte): Tipo de comando (Ej. 0x01 = Prioridad de Emergencia, 0x02 = Cambio de Fase).
   * `Intersection ID` (4 bytes): Identificador del semáforo.
   * `Timestamp` (4 bytes): Unix timestamp (segundos).
   * `Action / Phase Payload` (8 bytes): Duración adicional de fase verde, número de fase a forzar, etc.
   * `Security Token / Checksum` (8 bytes): Firma criptográfica liviana y CRC32 para verificación de integridad y prevención de manipulación.
3. **Control de Flujo y Backpressure**: El adaptador contará con colas locales de prioridad para mensajes de emergencia (los cuales se procesan inmediatamente saltando la cola de mensajes ordinarios de optimización).

## Consecuencias
* **Positivas**:
  * Cumplimiento estricto de las limitaciones del hardware heredado (mensajes compactos < 256 bytes).
  * Aislamiento: Los microservicios modernos de tráfico y ruteo no necesitan conocer los detalles del protocolo de bajo nivel NTCIP; solo publican eventos genéricos a Kafka.
  * Mayor seguridad al empaquetar una firma de integridad CRC32/HMAC dentro de la restricción de tamaño.
* **Negativas**:
  * Requiere un esfuerzo de desarrollo adicional para codificar y decodificar mensajes binarios de bajo nivel de forma rápida y sin errores.
  * Dificultad para debuggear de forma visual (se requieren herramientas de introspección binaria en el adaptador).
