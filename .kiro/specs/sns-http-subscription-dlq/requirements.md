# Requirements Document

## Introduction

Esta feature extiende la funcionalidad existente de suscripciones HTTP de SNS en el fork de MassTransit para crear automáticamente una cola SQS Dead Letter Queue (DLQ) cuando se configura una suscripción HTTP. La DLQ captura mensajes que SNS no puede entregar al endpoint HTTP/HTTPS, proporcionando resiliencia y observabilidad para entregas fallidas. Se configura mediante el atributo `RedrivePolicy` de la suscripción SNS, que apunta al ARN de la cola SQS DLQ.

Restricción crítica: todos los cambios deben ser aditivos — no se modifica código original de MassTransit.

## Glossary

- **DLQ_Queue**: Cola SQS que recibe mensajes que SNS no pudo entregar al endpoint HTTP después de agotar los reintentos configurados.
- **RedrivePolicy**: Atributo JSON de la suscripción SNS que especifica el ARN de la DLQ y el número máximo de reintentos (`maxReceiveCount` en el contexto de SNS se traduce a `deadLetterTargetArn` + número de reintentos de entrega).
- **Http_Subscription_Configurator**: Componente (`IHttpTopicSubscriptionConfigurator` / `HttpTopicSubscriptionConfigurator`) que permite configurar los parámetros de una suscripción HTTP de SNS.
- **Topology_Specification**: Componente (`HttpSubscriptionConsumeTopologySpecification`) que aplica la topología de la suscripción HTTP al broker topology builder.
- **Client_Context**: Componente (`AmazonSqsClientContext`) que ejecuta las llamadas a las APIs de AWS (SNS y SQS).
- **Topology_Filter**: Componente (`ConfigureAmazonSqsTopologyFilter`) que orquesta la creación de recursos AWS al iniciar el bus.
- **SNS_Subscription_ARN**: Identificador único de la suscripción SNS retornado por la API `SubscribeAsync`.
- **MaxReceiveCount**: Número máximo de intentos de entrega que SNS realizará antes de enviar el mensaje a la DLQ.

## Requirements

### Requirement 1: Configuración de DLQ en el Configurador de Suscripción HTTP

**User Story:** Como desarrollador, quiero poder habilitar y configurar una DLQ para mi suscripción HTTP de SNS a través del configurador existente, para que los mensajes no entregados se capturen automáticamente.

#### Acceptance Criteria

1. THE Http_Subscription_Configurator SHALL expose una propiedad `DeadLetterQueueEnabled` de tipo booleano con valor por defecto `false`.
2. THE Http_Subscription_Configurator SHALL expose una propiedad `DeadLetterQueueName` de tipo string nullable que permita especificar un nombre personalizado para la cola DLQ.
3. WHEN `DeadLetterQueueEnabled` es `true` y `DeadLetterQueueName` es null, vacío o compuesto únicamente por espacios en blanco, THE Http_Subscription_Configurator SHALL generar el nombre de la DLQ usando el patrón `{TopicName}-http-dlq`.
4. THE Http_Subscription_Configurator SHALL expose una propiedad `MaxReceiveCount` de tipo entero con valor por defecto `3`.
5. IF `MaxReceiveCount` se configura con un valor fuera del rango 1–100 (inclusive), THEN THE Http_Subscription_Configurator SHALL lanzar una `ArgumentOutOfRangeException` indicando el rango válido.
6. IF `DeadLetterQueueName` se configura con un valor que excede 80 caracteres o contiene caracteres distintos a alfanuméricos, guiones (`-`) y guiones bajos (`_`), THEN THE Http_Subscription_Configurator SHALL lanzar una `ArgumentException` indicando la restricción violada.
7. WHILE `DeadLetterQueueEnabled` es `false`, THE Http_Subscription_Configurator SHALL ignorar los valores de `DeadLetterQueueName` y `MaxReceiveCount` sin lanzar excepciones de validación al momento de construir la topología.

### Requirement 2: Creación de la Cola SQS DLQ

**User Story:** Como desarrollador, quiero que la cola SQS DLQ se cree automáticamente al iniciar el bus cuando la DLQ está habilitada, para no tener que crear recursos manualmente en AWS.

#### Acceptance Criteria

1. IF `DeadLetterQueueEnabled` es `true`, THEN THE Topology_Specification SHALL registrar la cola DLQ en el broker topology builder invocando la creación de cola con el nombre resuelto de la DLQ.
2. THE DLQ_Queue SHALL crearse como una cola SQS estándar (no FIFO), sin el atributo `FifoQueue` ni el sufijo `.fifo` en el nombre.
3. THE DLQ_Queue SHALL heredar la propiedad `Durable` de la configuración de la suscripción HTTP.
4. THE DLQ_Queue SHALL heredar la propiedad `AutoDelete` de la configuración de la suscripción HTTP.
5. WHEN la DLQ_Queue ya existe en AWS, THE Client_Context SHALL reutilizar la cola existente retornando su información (ARN y URL) sin lanzar excepciones.
6. THE Topology_Filter SHALL crear la DLQ_Queue en la fase de creación de colas (antes de la fase de creación de suscripciones HTTP) para garantizar que el ARN esté disponible para el RedrivePolicy.
7. THE DLQ_Queue SHALL crearse con el atributo `MessageRetentionPeriod` de 2592000 segundos (30 días).
8. IF la creación de la DLQ_Queue falla por un error de AWS, THEN THE Topology_Filter SHALL propagar la excepción incluyendo el nombre de la cola DLQ que no pudo crearse.
9. IF `DeadLetterQueueEnabled` es `false`, THEN THE Topology_Specification SHALL omitir la creación de la cola DLQ en el broker topology builder.

### Requirement 3: Configuración del RedrivePolicy en la Suscripción SNS

**User Story:** Como desarrollador, quiero que la suscripción SNS se configure automáticamente con un RedrivePolicy apuntando a la DLQ, para que SNS redirija mensajes fallidos a la cola DLQ.

#### Acceptance Criteria

1. WHEN `DeadLetterQueueEnabled` es `true` y la suscripción SNS retorna un ARN válido distinto de `PendingConfirmation`, THE Client_Context SHALL invocar `SetSubscriptionAttributesAsync` con el atributo `RedrivePolicy` en la suscripción, usando el ARN de la DLQ_Queue como destino.
2. THE Client_Context SHALL configurar el valor del atributo `RedrivePolicy` como un JSON con la estructura exacta `{"deadLetterTargetArn":"<ARN de la DLQ>"}`.
3. WHEN la suscripción SNS retorna el ARN `PendingConfirmation`, THE Client_Context SHALL omitir la invocación de `SetSubscriptionAttributesAsync` para el RedrivePolicy y registrar un mensaje de advertencia de nivel Warn indicando que el RedrivePolicy se aplicará después de la confirmación de la suscripción.
4. IF la llamada a `SetSubscriptionAttributesAsync` para configurar el RedrivePolicy falla con una excepción, THEN THE Client_Context SHALL registrar el error a nivel Error y lanzar una excepción que incluya el ARN de la suscripción y el ARN de la DLQ en el mensaje.
5. WHEN `DeadLetterQueueEnabled` es `false`, THE Client_Context SHALL omitir toda configuración de RedrivePolicy en la suscripción SNS.

### Requirement 4: Permisos IAM entre SNS y la Cola DLQ

**User Story:** Como desarrollador, quiero que la política de acceso de la cola DLQ se configure automáticamente para permitir que SNS envíe mensajes, para que la integración funcione sin configuración manual de permisos.

#### Acceptance Criteria

1. WHEN la DLQ_Queue se crea o se reutiliza, THE Client_Context SHALL verificar que la política de la cola contenga un statement con Effect=Allow, Action=`sqs:SendMessage`, Principal del servicio `sns.amazonaws.com`, y una condición `ArnLike` en `aws:SourceArn` que coincida con el ARN del topic SNS correspondiente.
2. WHEN la política de la DLQ_Queue no contiene un statement que permita `sqs:SendMessage` desde el servicio `sns.amazonaws.com` con condición `ArnLike` en `aws:SourceArn` para el ARN del topic correspondiente, THE Client_Context SHALL agregar el statement con Effect=Allow, Action=`sqs:SendMessage`, Principal=Service `sns.amazonaws.com`, Resource=ARN de la DLQ_Queue, y condición `ArnLike` en `aws:SourceArn` restringida al ARN del topic SNS específico, y persistir la política actualizada mediante la API SetQueueAttributes.
3. WHEN la política ya contiene un statement válido con Effect=Allow, Action=`sqs:SendMessage`, Principal=Service `sns.amazonaws.com`, y condición `ArnLike` en `aws:SourceArn` que incluya el ARN del topic SNS correspondiente, THE Client_Context SHALL omitir la actualización de la política sin invocar SetQueueAttributes.
4. WHILE se ejecuta la verificación y actualización de la política de la DLQ_Queue, THE Client_Context SHALL serializar el acceso concurrente de modo que solo una operación de actualización de política se ejecute a la vez por instancia de cola.
5. IF la invocación a SetQueueAttributes falla, THEN THE Client_Context SHALL propagar la excepción al llamador sin modificar la política en memoria.

### Requirement 5: Validación de la Configuración

**User Story:** Como desarrollador, quiero que la configuración de la DLQ se valide al construir la topología, para detectar errores de configuración antes de intentar crear recursos en AWS.

#### Acceptance Criteria

1. WHEN `DeadLetterQueueEnabled` es `true` y el nombre final de la DLQ (generado mediante el patrón `{TopicName}-http-dlq` o proporcionado explícitamente vía `DeadLetterQueueName`) excede 80 caracteres, THE Topology_Specification SHALL producir un `ValidationResult` con disposición `Failure` que identifique el campo `DeadLetterQueueName` y describa que el nombre excede el límite de 80 caracteres.
2. WHEN `DeadLetterQueueEnabled` es `true` y el nombre de la DLQ contiene caracteres distintos a letras ASCII (a-z, A-Z), dígitos (0-9), guiones (-) y guiones bajos (_), THE Topology_Specification SHALL producir un `ValidationResult` con disposición `Failure` que identifique el campo `DeadLetterQueueName` e indique los caracteres permitidos.
3. WHEN `DeadLetterQueueEnabled` es `false`, THE Topology_Specification SHALL omitir toda validación y creación relacionada con la DLQ, sin producir ningún `ValidationResult` referente a la DLQ.
4. WHEN `DeadLetterQueueEnabled` es `true` y el nombre resultante de la DLQ es vacío o consiste solo en espacios en blanco, THE Topology_Specification SHALL producir un `ValidationResult` con disposición `Failure` que identifique el campo `DeadLetterQueueName` e indique que el nombre no debe estar vacío.

### Requirement 6: Extensión del Método de Extensión Principal

**User Story:** Como desarrollador, quiero que la configuración de DLQ sea accesible a través del mismo flujo de configuración `SubscribeTopicToHttpEndpoint`, para mantener una API consistente y simple.

#### Acceptance Criteria

1. THE `SubscribeTopicToHttpEndpoint` extension method SHALL pasar las propiedades `DeadLetterQueueEnabled`, `DeadLetterQueueName` y `MaxReceiveCount` del Http_Subscription_Configurator al constructor de Topology_Specification.
2. WHEN se usa el overload genérico `SubscribeTopicToHttpEndpoint<T>`, THE extension method SHALL delegar al overload con nombre de topic explícito, garantizando que la configuración de DLQ se propague de forma idéntica.
3. IF `DeadLetterQueueEnabled` es `false`, THEN THE `SubscribeTopicToHttpEndpoint` extension method SHALL crear el Topology_Specification sin incluir parámetros de DLQ, produciendo el mismo comportamiento observable que antes de la introducción de la funcionalidad de DLQ (ninguna cola DLQ creada, ningún RedrivePolicy configurado).
4. THE `SubscribeTopicToHttpEndpoint` extension method SHALL mantener compatibilidad hacia atrás: las invocaciones existentes sin delegado `configure` o sin configurar propiedades de DLQ SHALL compilar y funcionar sin cambios.

### Requirement 7: Observabilidad y Logging

**User Story:** Como operador, quiero que la creación y configuración de la DLQ genere logs informativos, para poder diagnosticar problemas de entrega.

#### Acceptance Criteria

1. WHEN la DLQ_Queue se crea exitosamente (queueInfo.Existing es false), THE Topology_Filter SHALL registrar un log de nivel Debug con el nombre de la cola (EntityName), su ARN y su URL.
2. WHEN el RedrivePolicy se configura exitosamente en la suscripción, THE Client_Context SHALL registrar un log de nivel Info con el ARN de la suscripción y el ARN de la DLQ.
3. WHEN la DLQ_Queue ya existe (queueInfo.Existing es true), THE Topology_Filter SHALL registrar un log de nivel Debug indicando que se reutiliza la cola existente, incluyendo el nombre de la cola (EntityName), su ARN y su URL.
4. IF la configuración del RedrivePolicy falla (se lanza una excepción durante la operación de SetSubscriptionAttributes), THEN THE Client_Context SHALL registrar un log de nivel Error que incluya el mensaje de la excepción, el ARN de la suscripción y el ARN de la DLQ.
5. THE Topology_Filter y THE Client_Context SHALL utilizar el formato de logging estructurado de MassTransit (LogContext.<Level>?.Log con placeholders nombrados entre llaves) para todos los logs de la DLQ.
