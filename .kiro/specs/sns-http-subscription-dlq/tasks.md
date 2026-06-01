# Implementation Plan

## Overview

Implementación incremental del soporte de Dead Letter Queue (DLQ) para suscripciones HTTP de SNS. Cada tarea extiende los componentes existentes de forma aditiva, sin modificar código original de MassTransit. Se sigue el orden del flujo de datos: configurador → topología → entidades → builder → client context → filter → extension method → tests.

## Tasks

- [x] 1. Extender interfaces y configurador con propiedades DLQ
  - [x] 1.1 Agregar propiedades DLQ a `IHttpTopicSubscriptionConfigurator`
    - Agregar `DeadLetterQueueEnabled` (bool, default false), `DeadLetterQueueName` (string?), y `MaxReceiveCount` (int, default 3) a la interfaz
    - Archivo: `src/Transports/MassTransit.AmazonSqsTransport/Configuration/IHttpTopicSubscriptionConfigurator.cs`
    - _Requirements: 1.1, 1.2, 1.3, 1.4_

  - [x] 1.2 Implementar propiedades DLQ en `HttpTopicSubscriptionConfigurator` con validación
    - Agregar backing fields, validación en setters (`MaxReceiveCount` 1-100 → `ArgumentOutOfRangeException`, `DeadLetterQueueName` ≤80 chars y `[a-zA-Z0-9\-_]` → `ArgumentException`)
    - Agregar método interno `ResolveDeadLetterQueueName()` que retorna `{TopicName}-http-dlq` si el nombre es null/whitespace
    - Agregar método privado estático `IsValidQueueName(string)`
    - Archivo: `src/Transports/MassTransit.AmazonSqsTransport/Configuration/HttpTopicSubscriptionConfigurator.cs`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7_

- [x] 2. Extender entidades de topología con propiedades DLQ
  - [x] 2.1 Agregar propiedades DLQ a la interfaz `HttpSubscription`
    - Agregar `DeadLetterQueueEnabled` (bool), `DeadLetterQueueName` (string?), `MaxReceiveCount` (int)
    - Archivo: `src/Transports/MassTransit.AmazonSqsTransport/AmazonSqsTransport/Topology/Entities/HttpSubscription.cs`
    - _Requirements: 1.1, 1.2, 1.4_

  - [x] 2.2 Extender `HttpSubscriptionEntity` con propiedades DLQ
    - Agregar parámetros opcionales al constructor (`deadLetterQueueEnabled = false`, `deadLetterQueueName = null`, `maxReceiveCount = 3`)
    - Implementar las nuevas propiedades de la interfaz
    - Archivo: `src/Transports/MassTransit.AmazonSqsTransport/AmazonSqsTransport/Topology/Entities/HttpSubscriptionEntity.cs`
    - _Requirements: 1.1, 1.2, 1.4_

- [x] 3. Extender topology builder con overload DLQ-aware
  - [x] 3.1 Agregar overload `CreateHttpSubscription` con parámetros DLQ a `IBrokerTopologyBuilder`
    - Nuevo método: `HttpSubscriptionHandle CreateHttpSubscription(TopicHandle topic, string endpointUrl, bool rawMessageDelivery, bool deadLetterQueueEnabled, string? deadLetterQueueName, int maxReceiveCount)`
    - Archivo: `src/Transports/MassTransit.AmazonSqsTransport/AmazonSqsTransport/Topology/IBrokerTopologyBuilder.cs`
    - _Requirements: 2.1_

  - [x] 3.2 Implementar el overload en `BrokerTopologyBuilder`
    - Crear `HttpSubscriptionEntity` con los parámetros DLQ y registrar en la colección
    - Archivo: `src/Transports/MassTransit.AmazonSqsTransport/AmazonSqsTransport/Topology/BrokerTopologyBuilder.cs`
    - _Requirements: 2.1_

- [x] 4. Extender `HttpSubscriptionConsumeTopologySpecification` con DLQ
  - [x] 4.1 Agregar constructor con parámetros DLQ y extender `Validate()` y `Apply()`
    - Nuevo constructor que acepta `deadLetterQueueEnabled`, `deadLetterQueueName`, `maxReceiveCount`
    - En `Validate()`: si DLQ habilitada, validar nombre resuelto (vacío, >80 chars, caracteres inválidos)
    - En `Apply()`: si DLQ habilitada, registrar cola con `builder.CreateQueue(dlqName, Durable, AutoDelete, queueAttributes)` donde `queueAttributes` incluye `MessageRetentionPeriod=2592000`
    - Usar el overload DLQ-aware de `CreateHttpSubscription` en lugar del existente
    - Archivo: `src/Transports/MassTransit.AmazonSqsTransport/Configuration/HttpSubscriptionConsumeTopologySpecification.cs`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.7, 2.9, 5.1, 5.2, 5.3, 5.4_

- [x] 5. Checkpoint - Verificar compilación y validación
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Extender `AmazonSqsClientContext.CreateHttpSubscription` con RedrivePolicy
  - [x] 6.1 Agregar overload de `CreateHttpSubscription` que acepta `dlqArn` y `maxReceiveCount`
    - Después de obtener `subscriptionArn` válido (no `PendingConfirmation`): invocar `SetSubscriptionAttributesAsync` con `RedrivePolicy` = `{"deadLetterTargetArn":"<dlqArn>"}`
    - Si `subscriptionArn == "PendingConfirmation"` y `dlqArn != null`: log Warning indicando que RedrivePolicy se aplicará después de confirmación
    - Si `SetSubscriptionAttributesAsync` falla: log Error y lanzar `InvalidOperationException` con ARNs en mensaje
    - Si `dlqArn` es null: comportamiento idéntico al método existente (sin RedrivePolicy)
    - Archivo: `src/Transports/MassTransit.AmazonSqsTransport/AmazonSqsTransport/AmazonSqsClientContext.cs`
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 7.2, 7.4, 7.5_

- [x] 7. Extender `ConfigureAmazonSqsTopologyFilter` con permisos IAM para DLQ
  - [x] 7.1 Modificar el método `Declare` para `HttpSubscription` con lógica DLQ
    - Si `subscription.DeadLetterQueueEnabled`: obtener `QueueInfo` de la DLQ, obtener `TopicInfo`, invocar `UpdatePolicy` para permisos IAM (`sqs:SendMessage` desde `sns.amazonaws.com` con condición `ArnLike`)
    - Pasar `dlqArn` al overload extendido de `CreateHttpSubscription`
    - Agregar logs Debug para creación/reutilización de DLQ, log Info para RedrivePolicy exitoso
    - Archivo: `src/Transports/MassTransit.AmazonSqsTransport/AmazonSqsTransport/Middleware/ConfigureAmazonSqsTopologyFilter.cs`
    - _Requirements: 2.5, 2.6, 2.8, 4.1, 4.2, 4.3, 4.4, 4.5, 7.1, 7.3, 7.5_

- [x] 8. Extender `AmazonSqsHttpSubscriptionExtensions` para pasar configuración DLQ
  - [x] 8.1 Actualizar `SubscribeTopicToHttpEndpoint` para propagar propiedades DLQ
    - Pasar `DeadLetterQueueEnabled`, `DeadLetterQueueName`, `MaxReceiveCount` del configurador al constructor de `HttpSubscriptionConsumeTopologySpecification`
    - Mantener compatibilidad hacia atrás: si DLQ no está habilitada, usar constructor sin parámetros DLQ o pasar defaults
    - Verificar que el overload genérico `<T>` delega correctamente
    - Archivo: `src/Transports/MassTransit.AmazonSqsTransport/Configuration/AmazonSqsHttpSubscriptionExtensions.cs`
    - _Requirements: 6.1, 6.2, 6.3, 6.4_

- [x] 9. Checkpoint - Verificar compilación completa
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Agregar dependencia FsCheck al proyecto de tests
  - [x] 10.1 Agregar `FsCheck` y `FsCheck.NUnit` a `Directory.Packages.props` y al `.csproj` de tests
    - Agregar `<PackageVersion Include="FsCheck" Version="3.1.0" />` y `<PackageVersion Include="FsCheck.NUnit" Version="3.1.0" />` en `Directory.Packages.props`
    - Agregar `<PackageReference Include="FsCheck" />` y `<PackageReference Include="FsCheck.NUnit" />` en el `.csproj` de tests
    - _Requirements: (infraestructura de testing)_

- [x] 11. Tests unitarios y property-based del configurador
  - [x] 11.1 Crear tests unitarios para `HttpTopicSubscriptionConfigurator` (valores por defecto y validación)
    - Verificar defaults: `DeadLetterQueueEnabled = false`, `MaxReceiveCount = 3`, `DeadLetterQueueName = null`
    - Verificar que DLQ deshabilitada no lanza excepciones al tener valores arbitrarios en las propiedades DLQ
    - Archivo: `tests/MassTransit.AmazonSqsTransport.Tests/HttpSubscription/HttpTopicSubscriptionConfiguratorTests.cs`
    - _Requirements: 1.1, 1.4, 1.7_

  - [x] 11.2 Property test: Generación del nombre de DLQ (Property 1)
    - **Property 1: Generación del nombre de DLQ sigue el patrón**
    - Para cualquier TopicName válido y DeadLetterQueueName null/whitespace, el nombre resuelto debe ser `{TopicName}-http-dlq`
    - **Validates: Requirements 1.3**

  - [x] 11.3 Property test: MaxReceiveCount rechaza valores fuera de rango (Property 2)
    - **Property 2: MaxReceiveCount rechaza valores fuera de rango**
    - Para cualquier int fuera de [1,100] → `ArgumentOutOfRangeException`; dentro de [1,100] → sin excepción
    - **Validates: Requirements 1.5**

  - [x] 11.4 Property test: DeadLetterQueueName rechaza nombres inválidos (Property 3)
    - **Property 3: DeadLetterQueueName rechaza nombres inválidos**
    - Para cualquier string con caracteres fuera de `[a-zA-Z0-9\-_]` o >80 chars → `ArgumentException`; válidos → sin excepción
    - **Validates: Requirements 1.6**

- [x] 12. Tests de topología y validación
  - [x] 12.1 Property test: DLQ deshabilitada no produce comportamiento DLQ (Property 4)
    - **Property 4: DLQ deshabilitada no produce comportamiento DLQ**
    - Para cualquier configuración con `DeadLetterQueueEnabled = false`, no se registra cola DLQ ni se producen ValidationResults de DLQ
    - **Validates: Requirements 1.7, 2.9, 5.3**

  - [x] 12.2 Property test: DLQ habilitada registra cola con propiedades heredadas (Property 5)
    - **Property 5: DLQ habilitada registra cola con propiedades heredadas**
    - Para cualquier configuración con DLQ habilitada, la cola registrada hereda `Durable` y `AutoDelete` de la suscripción
    - **Validates: Requirements 2.1, 2.3, 2.4**

  - [x] 12.3 Property test: Validación de topología rechaza nombres de DLQ inválidos (Property 7)
    - **Property 7: Validación de topología rechaza nombres de DLQ inválidos**
    - Para cualquier nombre resuelto >80 chars, con caracteres inválidos, o vacío → `ValidationResult` con Failure en `DeadLetterQueueName`
    - **Validates: Requirements 5.1, 5.2, 5.4**

  - [x] 12.4 Tests unitarios para `HttpSubscriptionConsumeTopologySpecification`
    - Verificar que cola DLQ es estándar (no FIFO, sin sufijo `.fifo`)
    - Verificar `MessageRetentionPeriod = 2592000` en atributos de la cola
    - Verificar que DLQ deshabilitada no registra cola
    - Archivo: `tests/MassTransit.AmazonSqsTransport.Tests/HttpSubscription/HttpSubscriptionTopologySpecificationTests.cs`
    - _Requirements: 2.2, 2.7, 2.9_

- [x] 13. Tests del RedrivePolicy y formato JSON
  - [x]* 13.1 Property test: Formato del RedrivePolicy JSON (Property 6)
    - **Property 6: Formato del RedrivePolicy JSON**
    - Para cualquier ARN válido de DLQ, el JSON generado debe ser exactamente `{"deadLetterTargetArn":"<ARN>"}` sin espacios ni campos extra
    - Archivo: `tests/MassTransit.AmazonSqsTransport.Tests/HttpSubscription/RedrivePolicyTests.cs`
    - **Validates: Requirements 3.2**

- [x] 14. Tests de integración con AWS mocked
  - [x]* 14.1 Test de integración: Creación completa DLQ + suscripción + RedrivePolicy
    - Mock de `IAmazonSimpleNotificationService` y `IAmazonSQS`
    - Verificar flujo completo: crear cola DLQ → configurar permisos → crear suscripción → aplicar RedrivePolicy
    - Verificar que `PendingConfirmation` omite RedrivePolicy y genera log Warning
    - Verificar que fallo de AWS propaga excepción con contexto (nombre de cola, ARNs)
    - Archivo: `tests/MassTransit.AmazonSqsTransport.Tests/HttpSubscription/HttpSubscriptionDlqIntegrationTests.cs`
    - _Requirements: 2.1, 2.5, 2.8, 3.1, 3.3, 3.4, 4.1, 4.2, 4.3_

  - [x]* 14.2 Test de integración: Compatibilidad hacia atrás del extension method
    - Verificar que invocaciones existentes sin configurar DLQ compilan y funcionan sin cambios
    - Verificar que el overload genérico `<T>` delega correctamente la configuración DLQ
    - Archivo: `tests/MassTransit.AmazonSqsTransport.Tests/HttpSubscription/HttpSubscriptionDlqIntegrationTests.cs`
    - _Requirements: 6.2, 6.3, 6.4_

- [x] 15. Checkpoint final - Verificar que todos los tests pasan
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Las tareas marcadas con `*` son opcionales y pueden omitirse para un MVP más rápido
- Cada tarea referencia los requisitos específicos para trazabilidad
- Los checkpoints aseguran validación incremental
- Los property tests validan propiedades universales de correctitud definidas en el diseño
- Los tests unitarios validan ejemplos específicos y casos borde
- **Restricción crítica**: Todos los cambios son aditivos — no se modifica código original de MassTransit
- FsCheck 3.x se usa como librería PBT (compatible con NUnit vía `FsCheck.NUnit`)
- La versión de FsCheck debe verificarse al momento de implementar (usar la última estable 3.x)

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "2.1"] },
    { "id": 1, "tasks": ["1.2", "2.2"] },
    { "id": 2, "tasks": ["3.1"] },
    { "id": 3, "tasks": ["3.2"] },
    { "id": 4, "tasks": ["4.1"] },
    { "id": 5, "tasks": ["6.1", "8.1"] },
    { "id": 6, "tasks": ["7.1"] },
    { "id": 7, "tasks": ["10.1"] },
    { "id": 8, "tasks": ["11.1", "11.2", "11.3", "11.4"] },
    { "id": 9, "tasks": ["12.1", "12.2", "12.3", "12.4"] },
    { "id": 10, "tasks": ["13.1"] },
    { "id": 11, "tasks": ["14.1", "14.2"] }
  ]
}
```
