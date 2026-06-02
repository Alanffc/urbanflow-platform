# PLATAFORMA INTELIGENTE DE MOVILIDAD URBANA - URBANFLOW
## TERCERA EVALUACIÓN SEMESTRAL - QUINTA HACKATON
**INTEGRANTE(S) / GRUPO:** Team PIN-8  
**UNIVERSIDAD:** Facultad de Ciencias y Tecnología  
**FECHA DE ENTREGA:** 02 de Junio de 2026  

---

## 1. Carátula y Presentación de la Entrega

* **Empresa Contratada:** UrbanFlow Technologies Ltda.
* **Cliente:** Alcaldía Metropolitana (Metrópolis de >5 Millones de Habitantes)
* **Equipo de Desarrollo (Team PIN-8):**
  * *Arquitecto de Software & DevOps Engineer:* Antigravity AI
  * *Miembros del Grupo:* [Por Completar por el Usuario]
* **Repositorios de GitHub del Grupo:**
  * Backend Monorepo: `https://github.com/team-pin8/urbanflow-platform`

---

## 2. Diagramas Arquitectónicos (Mermaid.js)

### A. Diagrama C4 (Nivel 2 - Contenedores)

Este diagrama representa cómo interactúan los contenedores (microservicios) y las bases de datos a través de la infraestructura orientada a eventos con Apache Kafka.

```mermaid
graph TD
    %% Users
    Citizen["Ciudadano (App Móvil / NFC / QR)"]
    BusDriver["Conductor de Bus (Terminal a Bordo)"]
    TrafficOperator["Operador del Centro de Control (Web SPA)"]
    TrafficLights["Semáforo Inteligente (Protocolo NTCIP)"]

    %% Gateway & Ingres
    APIGateway["API Gateway (Express / Kong)"]

    %% Event Bus
    subgraph EventStreaming["Event Streaming Backbone (Apache Kafka)"]
        TelemetryTopic["Topic: telemetry-gps"]
        PaymentTopic["Topic: payment-events"]
        RerouteTopic["Topic: reroute-events"]
        CongestionTopic["Topic: congestion-events"]
        NTCIPTopic["Topic: ntcip-commands"]
    end

    %% Microservices
    subgraph Microservices["Capa de Microservicios (Node.js + TypeScript)"]
        TelemetryService["Telemetry Microservice"]
        RoutingService["Routing Microservice"]
        PaymentService["Payment Microservice"]
        TrafficService["Traffic & NTCIP Adapter"]
        PredictionEngine["Prediction Engine (Python/Node)"]
        NotificationService["Notification Microservice"]
    end

    %% Databases
    subgraph Databases["Persistencia Políglota"]
        InfluxDB[("InfluxDB (Telemetry Time-Series)")]
        Neo4j[("Neo4j (Routing Graph DB)")]
        Postgres[("PostgreSQL (Payment Relational DB)")]
    end

    %% Relations Users to Gateway
    Citizen -->|Planifica Rutas / Paga| APIGateway
    BusDriver -->|Ubicación / Notificaciones| APIGateway
    TrafficOperator -->|Gestión / Monitoreo| APIGateway

    %% Relations Gateway to Services
    APIGateway -->|REST / WebSockets| TelemetryService
    APIGateway -->|REST| RoutingService
    APIGateway -->|REST| PaymentService
    APIGateway -->|REST / WebSockets| TrafficService

    %% Microservices to DBs
    TelemetryService -->|Escribe métricas| InfluxDB
    RoutingService -->|Consulta red vial| Neo4j
    PaymentService -->|Transacciones ACID| Postgres

    %% Microservices to Kafka
    TelemetryService -->|Publica GPS| TelemetryTopic
    PredictionEngine -->|Consume GPS / Predice Congestión| TelemetryTopic
    PredictionEngine -->|Publica Congestión| CongestionTopic
    
    TrafficService -->|Consume Congestión / Re-rutea| CongestionTopic
    TrafficService -->|Publica Prioridad Semáforos| NTCIPTopic
    TrafficService -->|Publica Re-ruteos| RerouteTopic
    
    RoutingService -->|Consume Re-ruteos| RerouteTopic
    
    PaymentService -->|Publica Pagos Completados| PaymentTopic
    
    NotificationService -->|Consume eventos de Reroute/Alertas| RerouteTopic
    NotificationService -->|Envía Push| Citizen
    NotificationService -->|Envía Alerta Terminal| BusDriver

    %% NTCIP Integration
    TrafficLights <-->|NTCIP (Max 256 bytes)| TrafficService
```

---

### B. Diagrama de Secuencia (Re-enrutamiento Automático - Inciso VI)

Este diagrama describe el flujo de punta a punta cuando un sensor GPS detecta una anomalía y desencadena la predicción de congestión y el re-enrutamiento automático del bus.

```mermaid
sequenceDiagram
    autonumber
    participant Bus as Sensor GPS (Bus)
    participant Kafka as Kafka Event Broker
    participant PE as Prediction Engine
    participant TS as Traffic Service
    participant RS as Routing Service
    participant NS as Notification Service
    participant Cond as Terminal del Conductor

    Note over Bus, Cond: Detección y Planificación de Re-enrutamiento en <10 Segundos
    Bus->>Kafka: Enviar posición en tiempo real (Topic: telemetry-gps)
    Kafka->>PE: Consume flujo continuo de posiciones GPS
    
    Note over PE: Analiza historial en Data Lake S3 & posiciones actuales.<br/>Detecta anomalía de congestión a 30 mins.
    
    PE->>Kafka: Publicar congestión detectada (Topic: congestion-events)
    Kafka->>TS: Consume alerta de congestión
    
    TS->>RS: Solicitar cálculo de ruta alternativa evitando congestión
    RS->>RS: Calcular ruta óptima en Grafo (Neo4j)
    RS-->>TS: Retornar nueva ruta óptima calculada
    
    TS->>Kafka: Publicar comando de re-enrutamiento (Topic: reroute-events)
    
    Note over TS: Genera auditoría regulatoria de cambios de ruta
    
    Kafka->>NS: Consume evento de re-enrutamiento
    Kafka->>Cond: Recibe desvío y actualiza mapa de navegación
    NS->>NS: Identificar pasajeros activos en el bus
    NS-->>Cond: Notificación push de desvío al Conductor
    NS-->>NS: Envía notificación push personalizada a ciudadanos afectados

---

### C. Diagrama de Despliegue en Nube (AWS Cloud Deployment)

Este diagrama modela la arquitectura de despliegue en producción utilizando servicios administrados de AWS para garantizar una disponibilidad del 99.95% y capacidad de procesamiento de 50,000 eventos/seg.

```mermaid
graph TB
    subgraph Clients["Capa de Clientes"]
        CitizenApp["App Móvil (iOS / Android)"]
        BusTerminal["Terminal de Bus (IoT)"]
        ControlRoom["Centro de Control (Web)"]
        LegacyTraffic["Semáforos NTCIP (Red 4G Privada)"]
    end

    subgraph AWS["Nube de AWS (Región Multi-AZ)"]
        %% Edge
        Route53["AWS Route 53 (DNS)"]
        ALB["AWS Application Load Balancer"]
        WAF["AWS WAF (Seguridad)"]
        
        %% API Gateway / Ingress
        APIGatewayAWS["AWS API Gateway / EKS Ingress Controller"]

        %% Compute
        subgraph EKS["AWS EKS (Kubernetes Cluster - Auto-scaling)"]
            PodTelemetry["Pods: Telemetry Service"]
            PodRouting["Pods: Routing Service"]
            PodPayment["Pods: Payment Service"]
            PodTraffic["Pods: Traffic & NTCIP Adapter"]
            PodPrediction["Pods: Prediction Engine"]
            PodNotification["Pods: Notification Service"]
        end

        %% Managed Event Streaming
        MSK["AWS MSK (Managed Streaming for Apache Kafka - Multi-AZ)"]

        %% Database Layer
        subgraph DataStores["Capa de Persistencia"]
            RDSPostgres["AWS RDS PostgreSQL (Multi-AZ Multi-Write)"]
            AuraDB["Neo4j AuraDB Enterprise / Cluster EC2"]
            InfluxDBEnterprise["InfluxDB Cloud Serverless / EC2 Cluster"]
        end

        %% Long-term Analytics & AI
        subgraph DataLake["AWS Data Lake"]
            KinesisFirehose["Amazon Kinesis Data Firehose"]
            S3Lake["Amazon S3 Bucket (10 Años de Datos Históricos)"]
            Glue["AWS Glue (Catalog & ETL)"]
            Athena["Amazon Athena (Consultas SQL Ad-hoc)"]
        end
    end

    %% Network Routing Paths
    CitizenApp & BusTerminal & ControlRoom --> Route53
    Route53 --> WAF
    WAF --> ALB
    ALB --> APIGatewayAWS
    APIGatewayAWS --> EKS
    
    %% NTCIP Adaptor VPN/DirectConnect Link
    LegacyTraffic <-->|IPSec VPN / Direct Connect| PodTraffic

    %% Services to Kafka
    PodTelemetry & PodPayment & PodTraffic & PodPrediction --> MSK
    MSK --> PodPrediction & PodTraffic & PodNotification

    %% Services to DBs
    PodTelemetry --> InfluxDBEnterprise
    PodRouting --> AuraDB
    PodPayment --> RDSPostgres

    %% Archiving to Data Lake
    MSK --> KinesisFirehose
    KinesisFirehose --> S3Lake
    Glue --> S3Lake
    Athena --> S3Lake
```

```

---

## 3. Modelo de Datos para el Data Lake Histórico (AWS S3)

Los datos se guardan en formato **Apache Parquet** indexados y particionados para búsquedas eficientes mediante queries en Amazon Athena y consumo para modelos de Machine Learning.

### Estructura de Particionamiento en S3:
* `s3://urbanflow-data-lake/telemetry/year=YYYY/month=MM/day=DD/`
* `s3://urbanflow-data-lake/payments/year=YYYY/month=MM/day=DD/`
* `s3://urbanflow-data-lake/reroutes-audit/year=YYYY/month=MM/`

### Diseños de Esquema (Ddl Athena / Parquet Metastore)

#### A. Tabla: `telemetry_history` (Datos de Sensores y Ubicación)
| Campo | Tipo | Descripción |
|---|---|---|
| `timestamp` | TIMESTAMP | Marca de tiempo exacta del evento |
| `vehicle_id` | STRING | ID del bus, tren, o scooter |
| `route_id` | STRING | ID de la ruta activa asignada |
| `latitude` | DOUBLE | Latitud del GPS |
| `longitude` | DOUBLE | Longitud del GPS |
| `speed_kmh` | FLOAT | Velocidad reportada por el sensor |
| `passenger_count` | INT | Número de pasajeros actual detectado por sensores de peso |
| `co2_emission_g_km` | FLOAT | Estimado de emisión de CO₂ por kilómetro recorrido |

#### B. Tabla: `trip_billing_history` (Trazabilidad y Auditoría de Tarifas - Contexto Adicional b)
| Campo | Tipo | Descripción |
|---|---|---|
| `trip_id` | STRING | ID único del viaje completo multimodal |
| `user_id` | STRING | ID del usuario (anonimizado usando Privacidad Diferencial en origen) |
| `start_time` | TIMESTAMP | Hora de inicio del viaje |
| `end_time` | TIMESTAMP | Hora de finalización del viaje |
| `fare_amount` | DECIMAL(10,2) | Costo total cobrado al usuario |
| `transport_modes` | ARRAY<STRING> | Lista de modos combinados: `['BUS', 'METRO', 'WALK']` |
| `payment_method` | STRING | Método utilizado: `NFC`, `QR`, `APP` |
| `tariff_version` | STRING | Hash identificador de la tarifa aprobada vigente para auditoría |

#### C. Tabla: `reroute_audit_history` (Trazabilidad de Re-enrutamientos - Contexto Adicional b)
| Campo | Tipo | Descripción |
|---|---|---|
| `event_id` | STRING | ID único del evento de re-enrutamiento |
| `timestamp` | TIMESTAMP | Hora exacta de la re-asignación |
| `vehicle_id` | STRING | ID del vehículo modificado |
| `original_route_id`| STRING | ID de la ruta anterior |
| `new_route_id` | STRING | ID de la nueva ruta recomendada |
| `trigger_reason` | STRING | Motivo del desvío (ej. `CONGESTION_30MIN_PREDICTION`, `ACCIDENT`) |
| `time_delta_seconds`| INT | Diferencia de tiempo estimado de llegada tras el cambio |
| `operator_id` | STRING | ID del operador que validó (o `SYSTEM` si fue automatizado) |

---

## 4. Instrucciones para Ejecución Local (Docker Compose)

### Requisitos previos:
* Docker instalado con Docker Compose v2.
* Node.js v20 o superior instalado localmente para desarrollo del microservicio.

### Pasos para levantar la Infraestructura local:
1. Clonar el repositorio.
2. Iniciar todos los contenedores e infraestructura requerida:
   ```bash
   docker compose up -d
   ```
3. Verificar que los servicios están listos:
   * **Kafka Broker**: `localhost:29092`
   * **PostgreSQL**: `localhost:5432` (Base de datos: `urbanflow_payments`)
   * **Neo4j**: `localhost:7474` (Consola Web) / `localhost:7687` (Bolt)
   * **InfluxDB**: `localhost:8086` (Consola de InfluxDB v2)

---

## 5. Arquitectura del Microservicio de Pagos (Payment Service)

El boilerplate estructurado con Express, TypeScript y KafkaJS se encuentra disponible en la ruta:
* `services/payment-service/`
  * [package.json](file:///c:/Proyectos/urbanflow-platform/services/payment-service/package.json) - Configuración del proyecto y dependencias de Kafka/Postgres.
  * [tsconfig.json](file:///c:/Proyectos/urbanflow-platform/services/payment-service/tsconfig.json) - Configuración estricta del compilador TypeScript.
  * [server.ts](file:///c:/Proyectos/urbanflow-platform/services/payment-service/src/server.ts) - Implementación del consumidor y productor de Kafka, pool de conexiones a la base de datos PostgreSQL, transacción ACID de cobro, y endpoint REST Express.
