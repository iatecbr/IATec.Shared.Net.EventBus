# SNS HTTP Subscription — Dead Letter Queue (DLQ)

## Visão Geral

A funcionalidade de DLQ para suscripciones HTTP de SNS permite capturar automaticamente mensagens que o SNS não consegue entregar ao endpoint HTTP/HTTPS após esgotar as tentativas de entrega. Quando habilitada, o sistema cria automaticamente uma fila SQS como Dead Letter Queue e configura o `RedrivePolicy` na subscription SNS.

**Comportamento automático ao habilitar DLQ:**
1. Cria uma fila SQS padrão (DLQ) com retenção de 30 dias
2. Configura permissões IAM para que o SNS possa enviar mensagens à DLQ
3. Aplica o `RedrivePolicy` na subscription SNS apontando para a DLQ

## Início Rápido

### Uso Básico (nome da DLQ gerado automaticamente)

```csharp
cfg.SubscribeTopicToHttpEndpoint("OrderCreated", "https://api.example.com/webhooks/orders", sub =>
{
    sub.DeadLetterQueueEnabled = true;
});
// Cria a DLQ: "OrderCreated-http-dlq"
```

### Com Nome Personalizado

```csharp
cfg.SubscribeTopicToHttpEndpoint("OrderCreated", "https://api.example.com/webhooks/orders", sub =>
{
    sub.DeadLetterQueueEnabled = true;
    sub.DeadLetterQueueName = "orders-webhook-dlq";
});
```

### Com MaxReceiveCount Personalizado

```csharp
cfg.SubscribeTopicToHttpEndpoint("OrderCreated", "https://api.example.com/webhooks/orders", sub =>
{
    sub.DeadLetterQueueEnabled = true;
    sub.MaxReceiveCount = 5; // SNS tentará 5 vezes antes de enviar à DLQ
});
```

### Usando o Overload Genérico

```csharp
cfg.SubscribeTopicToHttpEndpoint<OrderCreatedEvent>("https://api.example.com/webhooks/orders", sub =>
{
    sub.DeadLetterQueueEnabled = true;
    sub.DeadLetterQueueName = "orders-dlq";
    sub.MaxReceiveCount = 10;
});
```

## Configuração Completa

```csharp
services.AddMassTransit(x =>
{
    x.UsingAmazonSqs((context, cfg) =>
    {
        cfg.Host(new Uri("amazonsqs://us-east-1"), h =>
        {
            h.Scope = "production";
            h.AccessKey = "...";
            h.SecretKey = "...";
        });

        // Subscription HTTP com DLQ habilitada
        cfg.SubscribeTopicToHttpEndpoint("PaymentProcessed",
            "https://api.example.com/webhooks/payments", sub =>
        {
            sub.RawMessageDelivery = false;  // Receber envelope SNS completo
            sub.Durable = true;
            sub.AutoDelete = false;
            sub.DeadLetterQueueEnabled = true;
            sub.DeadLetterQueueName = "payments-http-dlq";
            sub.MaxReceiveCount = 3;
        });

        cfg.ConfigureEndpoints(context);
    });
});
```

## Exemplo Completo: Consumer HTTP com DLQ

Diferente de um consumer MassTransit tradicional (`IConsumer<T>`), a subscription HTTP faz com que o SNS envie POSTs diretamente ao seu endpoint. Você precisa de um **controller ASP.NET Core** para receber essas mensagens.

### 1. Evento (contrato compartilhado)

```csharp
namespace MyApp.Contracts;

public record OrderCreatedEvent
{
    public Guid OrderId { get; init; }
    public string CustomerName { get; init; }
    public decimal Total { get; init; }
    public DateTime CreatedAt { get; init; }
}
```

### 2. Program.cs — Registro de serviços e configuração do bus

```csharp
using MassTransit;
using MassTransit.AmazonSqsTransport;
using MyApp.Contracts;

var builder = WebApplication.CreateBuilder(args);

// Registrar o handler de confirmação SNS (obrigatório)
builder.Services.AddSnsSubscriptionConfirmation();

builder.Services.AddControllers();

builder.Services.AddMassTransit(x =>
{
    x.UsingAmazonSqs((context, cfg) =>
    {
        cfg.Host(new Uri("amazonsqs://us-east-1"), h =>
        {
            h.Scope = "production";
            h.AccessKey = builder.Configuration["AWS:AccessKey"];
            h.SecretKey = builder.Configuration["AWS:SecretKey"];
        });

        // Subscription HTTP com DLQ
        cfg.SubscribeTopicToHttpEndpoint<OrderCreatedEvent>(
            "https://api.myapp.com/webhooks/orders", sub =>
        {
            sub.RawMessageDelivery = false; // Necessário para receber o envelope SNS
            sub.DeadLetterQueueEnabled = true;
            sub.MaxReceiveCount = 5;
        });

        cfg.ConfigureEndpoints(context);
    });
});

var app = builder.Build();
app.MapControllers();
app.Run();
```

### 3. Controller — Recebendo mensagens do SNS

```csharp
using MassTransit.AmazonSqsTransport;
using MassTransit.Serialization;
using Microsoft.AspNetCore.Mvc;
using MyApp.Contracts;

namespace MyApp.Controllers;

[ApiController]
[Route("webhooks")]
public class SnsWebhookController : ControllerBase
{
    readonly ILogger<SnsWebhookController> _logger;

    public SnsWebhookController(ILogger<SnsWebhookController> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Endpoint que recebe POSTs do SNS.
    /// O atributo [SnsSubscriptionConfirmation] cuida automaticamente de:
    /// - Confirmar a subscription (SubscriptionConfirmation)
    /// - Extrair o SnsEnvelope e o MessageEnvelope do MassTransit
    /// </summary>
    [HttpPost("orders")]
    [SnsSubscriptionConfirmation]
    public IActionResult HandleOrderCreated()
    {
        // Opção 1: Extrair o envelope MassTransit completo (com headers, messageId, etc.)
        var messageEnvelope = HttpContext.GetMessageEnvelope();
        if (messageEnvelope == null)
        {
            _logger.LogWarning("Received SNS notification without MassTransit envelope");
            return Ok(); // Retornar 200 para o SNS não reenviar
        }

        // Opção 2: Extrair o envelope SNS (TopicArn, MessageId, Timestamp, etc.)
        var snsEnvelope = HttpContext.GetSnsEnvelope();

        _logger.LogInformation(
            "Received message {MessageId} from topic {TopicArn}",
            snsEnvelope?.MessageId, snsEnvelope?.TopicArn);

        // Deserializar o payload tipado do SNS envelope
        var orderEvent = snsEnvelope?.DeserializeMessage<OrderCreatedEvent>();

        if (orderEvent != null)
        {
            _logger.LogInformation(
                "Order {OrderId} created for {Customer}, total: {Total}",
                orderEvent.OrderId, orderEvent.CustomerName, orderEvent.Total);

            // Sua lógica de negócio aqui...
        }

        return Ok();
    }
}
```

### 4. Alternativa — Filter global (sem atributo por action)

```csharp
// Program.cs — registrar o filter globalmente
builder.Services.AddControllers(options =>
{
    options.Filters.Add<SnsSubscriptionConfirmationFilter>();
});
```

```csharp
// Controller simplificado (sem [SnsSubscriptionConfirmation])
[HttpPost("orders")]
public IActionResult HandleOrderCreated()
{
    var envelope = HttpContext.GetSnsEnvelope();
    var order = envelope?.DeserializeMessage<OrderCreatedEvent>();

    // processar...
    return Ok();
}
```

### 5. Reprocessando mensagens da DLQ

A DLQ entra em ação automaticamente quando o SNS não consegue entregar ao endpoint (timeout, 5xx, endpoint offline). Para reprocessar mensagens da DLQ, crie um consumer SQS tradicional:

```csharp
public class OrderCreatedDlqConsumer : IConsumer<OrderCreatedEvent>
{
    readonly ILogger<OrderCreatedDlqConsumer> _logger;

    public OrderCreatedDlqConsumer(ILogger<OrderCreatedDlqConsumer> logger)
    {
        _logger = logger;
    }

    public async Task Consume(ConsumeContext<OrderCreatedEvent> context)
    {
        _logger.LogWarning(
            "Reprocessing failed delivery for Order {OrderId}",
            context.Message.OrderId);

        // Reprocessar mensagem que falhou na entrega HTTP...
    }
}

// Registrar o consumer na fila DLQ
cfg.ReceiveEndpoint("production_OrderCreatedEvent-http-dlq", e =>
{
    e.ConfigureConsumer<OrderCreatedDlqConsumer>(context);
});
```

### Fluxo Resumido

```
SNS Topic ──POST──▶ /webhooks/orders
                         │
                         ▼
              SnsSubscriptionConfirmationFilter
                         │
                    ┌─────┴─────┐
                    │           │
            SubscriptionConfirmation    Notification
            (auto-confirmado, 200 OK)   │
                                        ▼
                              Controller Action
                              (sua lógica)
                                        │
                              ┌─────────┴─────────┐
                              │                   │
                         Sucesso (200)      Falha (5xx/timeout)
                                                  │
                                          SNS retenta (até MaxReceiveCount)
                                                  │
                                          Esgotou tentativas?
                                                  │
                                            ▼ Sim
                                     Mensagem → DLQ (SQS)
```

### 6. Resolução Dinâmica de Mensagens (`ResolveMessage`)

O método `ResolveMessage()` resolve o tipo .NET a partir do `messageType` do envelope MassTransit e deserializa automaticamente. Ideal para integração com MediatR:

```csharp
using MassTransit.AmazonSqsTransport;
using MediatR;
using Microsoft.AspNetCore.Mvc;

[ApiController]
[Route("webhooks")]
public class SnsWebhookController : ControllerBase
{
    readonly IMediator _mediator;

    public SnsWebhookController(IMediator mediator) => _mediator = mediator;

    [HttpPost("events")]
    [SnsSubscriptionConfirmation]
    public async Task<IActionResult> Handle()
    {
        // Resolve o tipo pelo messageType URN e deserializa
        var (type, message) = HttpContext.ResolveMessage();

        if (message == null)
            return BadRequest("Unknown message type");

        // Dispatch via MediatR (funciona com IRequest ou INotification)
        await _mediator.Send((dynamic)message);
        return Ok();
    }
}
```

**Métodos disponíveis no `HttpContext`:**

| Método | Retorno | Uso |
|---|---|---|
| `GetMessageEnvelope()` | `MessageEnvelope?` | Acesso ao envelope completo (headers, messageType, etc.) |
| `ResolveMessage()` | `(Type?, object?)` | Resolução dinâmica pelo `messageType` URN |
| `GetMessage<T>()` | `T?` | Deserialização tipada (quando sabe o tipo) |
| `GetRawPayload()` | `string?` | Body raw (quando `RawMessageDelivery=true` e não é envelope SNS) |

**Como funciona o `ResolveMessage()`:**
1. Lê o array `messageType` do envelope (ex: `["urn:message:MyApp.Contracts:OrderCreatedEvent"]`)
2. Converte o URN para nome de tipo .NET (`MyApp.Contracts.OrderCreatedEvent`)
3. Busca o tipo nos assemblies carregados
4. Deserializa o campo `message` para esse tipo
5. Retorna a tupla `(Type, object)` — pronto para cast ou dispatch dinâmico

## Referência da API

### `IHttpTopicSubscriptionConfigurator`

| Propriedade | Tipo | Default | Descrição |
|---|---|---|---|
| `DeadLetterQueueEnabled` | `bool` | `false` | Habilita a criação automática da DLQ |
| `DeadLetterQueueName` | `string?` | `null` | Nome personalizado da fila DLQ. Se null/vazio, usa `{TopicName}-http-dlq` |
| `MaxReceiveCount` | `int` | `3` | Número máximo de tentativas de entrega (1–100) |
| `MinDelayTarget` | `int` | `20` | Delay mínimo (em segundos) entre tentativas de entrega (1–3600) |
| `MaxDelayTarget` | `int` | `20` | Delay máximo (em segundos) entre tentativas de entrega (1–3600) |
| `BackoffFunction` | `string` | `"linear"` | Função de backoff para os delays entre retries |

### Valores de `BackoffFunction`

| Valor | Comportamento |
|---|---|
| `"linear"` | Delay constante entre tentativas (default) |
| `"arithmetic"` | Delay cresce linearmente a cada tentativa |
| `"geometric"` | Delay dobra a cada tentativa |
| `"exponential"` | Delay cresce exponencialmente |

### Exemplo com Delivery Policy Personalizada

```csharp
cfg.SubscribeTopicToHttpEndpoint<OrderCreatedEvent>(
    "https://api.myapp.com/webhooks/orders", sub =>
{
    sub.DeadLetterQueueEnabled = true;
    sub.MaxReceiveCount = 5;             // 5 tentativas antes de ir pra DLQ
    sub.MinDelayTarget = 30;             // Mínimo 30 segundos entre retries
    sub.MaxDelayTarget = 300;            // Máximo 5 minutos entre retries
    sub.BackoffFunction = "exponential"; // Delay cresce exponencialmente
});
```

Isso gera o seguinte `DeliveryPolicy` na subscription SNS:

```json
{
  "healthyRetryPolicy": {
    "numRetries": 5,
    "minDelayTarget": 30,
    "maxDelayTarget": 300,
    "numMinDelayRetries": 0,
    "numMaxDelayRetries": 0,
    "numNoDelayRetries": 0,
    "backoffFunction": "exponential"
  }
}
```

> **Nota sobre LocalStack:** O LocalStack aceita a configuração do `DeliveryPolicy` mas **não implementa os retries com delay**. No LocalStack, a mensagem vai direto para a DLQ no primeiro erro. Os retries só funcionam na **AWS real**.

### Propriedades Existentes (não-DLQ)

| Propriedade | Tipo | Default | Descrição |
|---|---|---|---|
| `TopicName` | `string` | — | Nome do topic SNS (read-only) |
| `EndpointUrl` | `string` | — | URL do endpoint HTTP/HTTPS |
| `RawMessageDelivery` | `bool` | `false` | Se `true`, SNS envia o body sem envelope JSON |
| `Durable` | `bool` | `true` | Se o topic sobrevive a restart do broker |
| `AutoDelete` | `bool` | `false` | Se o topic é deletado ao fechar a conexão |

## Validações

### No Configurador (fail-fast no setter)

| Regra | Exceção |
|---|---|
| `MaxReceiveCount` fora de [1, 100] | `ArgumentOutOfRangeException` |
| `MinDelayTarget` fora de [1, 3600] | `ArgumentOutOfRangeException` |
| `MaxDelayTarget` fora de [1, 3600] | `ArgumentOutOfRangeException` |
| `DeadLetterQueueName` > 80 caracteres | `ArgumentException` |
| `DeadLetterQueueName` com caracteres inválidos | `ArgumentException` |

**Caracteres válidos para `DeadLetterQueueName`:** letras (a-z, A-Z), dígitos (0-9), hífens (`-`) e underscores (`_`).

### Na Topologia (validação ao iniciar o bus)

Se `DeadLetterQueueEnabled = true` e o nome resolvido da DLQ é inválido (vazio, >80 chars, caracteres inválidos), o bus falhará ao iniciar com um `ValidationResult` descrevendo o problema.

> **Nota:** Quando `DeadLetterQueueEnabled = false`, nenhuma validação de DLQ é executada, independentemente dos valores de `DeadLetterQueueName` e `MaxReceiveCount`.

## Convenção de Nomes

| Cenário | Nome da DLQ |
|---|---|
| `DeadLetterQueueName = null` | `{scope_}{TopicName}-http-dlq` |
| `DeadLetterQueueName = ""` | `{scope_}{TopicName}-http-dlq` |
| `DeadLetterQueueName = "   "` | `{scope_}{TopicName}-http-dlq` |
| `DeadLetterQueueName = "my-custom-dlq"` | `{scope_}my-custom-dlq` |

### Scope Prefix

O scope configurado no host é automaticamente aplicado como prefixo tanto no topic quanto na DLQ:

```csharp
cfg.Host(new Uri("amazonsqs://us-east-1"), h =>
{
    h.Scope = "dev";  // Prefixo aplicado a topics e DLQs
});

cfg.SubscribeTopicToHttpEndpoint("OrderCreated", "https://...", sub =>
{
    sub.DeadLetterQueueEnabled = true;
});
// Topic criado: dev_OrderCreated
// DLQ criada:   dev_OrderCreated-http-dlq
```

### KebabCaseEndpointNameFormatter

Quando `SetKebabCaseEndpointNameFormatter` está ativo e você usa o overload genérico `SubscribeTopicToHttpEndpoint<T>`, o nome do topic (e consequentemente da DLQ) é formatado em kebab-case:

```csharp
cfg.SetKebabCaseEndpointNameFormatter();

cfg.Host(new Uri("amazonsqs://us-east-1"), h =>
{
    h.Scope = "dev";
});

cfg.SubscribeTopicToHttpEndpoint<OrderCreatedEvent>("https://...", sub =>
{
    sub.DeadLetterQueueEnabled = true;
});
// Topic criado: dev_order-created-event
// DLQ criada:   dev_order-created-event-http-dlq
```

> **Nota:** O `KebabCaseEndpointNameFormatter` só afeta o nome quando se usa o overload genérico `<T>`. Se você passa o topic name como string explícita, ele é usado como está (sem transformação de case).

## Recursos AWS Criados

Quando `DeadLetterQueueEnabled = true`, os seguintes recursos são criados/configurados automaticamente:

### 1. Fila SQS (DLQ)

- **Tipo:** Standard (não FIFO)
- **MessageRetentionPeriod:** 2.592.000 segundos (30 dias)
- **Durable/AutoDelete:** Herda da configuração da subscription

### 2. Política IAM na DLQ

```json
{
  "Effect": "Allow",
  "Principal": { "Service": "sns.amazonaws.com" },
  "Action": "sqs:SendMessage",
  "Resource": "arn:aws:sqs:{region}:{account}:{dlq-name}",
  "Condition": {
    "ArnLike": {
      "aws:SourceArn": "arn:aws:sns:{region}:{account}:{topic-name}"
    }
  }
}
```

### 3. RedrivePolicy na Subscription SNS

```json
{
  "deadLetterTargetArn": "arn:aws:sqs:{region}:{account}:{dlq-name}"
}
```

## Comportamento com PendingConfirmation

Quando uma subscription HTTP é criada pela primeira vez, o SNS retorna o status `PendingConfirmation` até que o endpoint confirme a subscription. Neste caso:

- O `RedrivePolicy` **não é aplicado** imediatamente
- Um log de **Warning** é emitido indicando que o RedrivePolicy será aplicado após a confirmação
- Na próxima inicialização do bus (após a confirmação), o RedrivePolicy será configurado normalmente

> **Importante:** Certifique-se de que seu endpoint HTTP processa o `SubscriptionConfirmation` do SNS usando `SnsSubscriptionConfirmationHandler`.

## Idempotência

A funcionalidade é idempotente:
- Se a fila DLQ já existe, ela é reutilizada (sem erro)
- Se a política IAM já contém o statement necessário, não é modificada
- Se o RedrivePolicy já está configurado, é reaplicado sem efeitos colaterais

## Observabilidade

### Logs Emitidos

| Nível | Mensagem | Quando |
|---|---|---|
| Debug | `Created DLQ {QueueName} (ARN: {Arn}, URL: {Url})` | DLQ criada com sucesso |
| Debug | `Reusing existing DLQ {QueueName} (ARN: {Arn}, URL: {Url})` | DLQ já existia |
| Info | `Configured RedrivePolicy on subscription {SubscriptionArn} -> DLQ {DlqArn}` | RedrivePolicy aplicado |
| Warning | `Cannot configure RedrivePolicy for pending subscription...` | Subscription em PendingConfirmation |
| Error | `Failed to configure RedrivePolicy on subscription {SubscriptionArn} -> DLQ {DlqArn}` | Falha ao aplicar RedrivePolicy |

### Monitoramento da DLQ

Para monitorar mensagens na DLQ, configure alarmes no CloudWatch:

```bash
# Verificar mensagens na DLQ
aws sqs get-queue-attributes \
  --queue-url https://sqs.us-east-1.amazonaws.com/123456789012/OrderCreated-http-dlq \
  --attribute-names ApproximateNumberOfMessages
```

**Métricas recomendadas para alarmes:**
- `ApproximateNumberOfMessages > 0` — Indica que há mensagens não entregues
- `ApproximateAgeOfOldestMessage` — Indica há quanto tempo a mensagem mais antiga está na DLQ

## Compatibilidade

- **Backward compatible:** Invocações existentes sem configurar DLQ continuam funcionando sem alterações
- **Opt-in:** A DLQ só é criada quando `DeadLetterQueueEnabled = true`
- **Sem breaking changes:** Nenhum código existente precisa ser modificado

## Permissões IAM Necessárias

O IAM role/user que executa a aplicação precisa das seguintes permissões adicionais quando DLQ está habilitada:

```json
{
  "Effect": "Allow",
  "Action": [
    "sqs:CreateQueue",
    "sqs:GetQueueUrl",
    "sqs:GetQueueAttributes",
    "sqs:SetQueueAttributes"
  ],
  "Resource": "arn:aws:sqs:*:*:*-http-dlq"
}
```

> **Nota:** Se você já usa MassTransit com SQS, provavelmente já possui essas permissões. A restrição de Resource pode ser ajustada conforme sua política de segurança.

## Troubleshooting

### A DLQ não está sendo criada

1. Verifique que `DeadLetterQueueEnabled = true` está configurado
2. Verifique os logs de nível Debug para mensagens sobre criação da DLQ
3. Confirme que o IAM role tem permissão `sqs:CreateQueue`

### RedrivePolicy não aplicado

1. Verifique se a subscription está em `PendingConfirmation` (veja logs Warning)
2. Confirme que o endpoint HTTP processou o `SubscriptionConfirmation`
3. Reinicie a aplicação após confirmar a subscription

### Erro de permissão ao configurar política da DLQ

1. Verifique que o IAM role tem `sqs:SetQueueAttributes`
2. Verifique os logs de nível Error para detalhes da exceção
3. Confirme que o ARN do topic SNS está correto

### Nome da DLQ inválido

O nome deve:
- Ter no máximo 80 caracteres
- Conter apenas: `a-z`, `A-Z`, `0-9`, `-`, `_`
- Não estar vazio

Se o nome gerado automaticamente (`{TopicName}-http-dlq`) exceder 80 caracteres, use `DeadLetterQueueName` para definir um nome mais curto.
