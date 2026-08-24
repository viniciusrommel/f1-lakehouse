# ADR-001: Apache Kafka como barramento de mensagens

**Status:** Accepted  
**Date:** 2024-06-28  
**Authors:** Data Platform Team

## Context

A plataforma precisa suportar ingestão de telemetria em alta frequência (~100 Hz por carro × 20 carros) com baixa latência ponta a ponta e capacidade de replay histórico para reprocessamento das camadas Prata e Ouro sem re-consumir a API externa.

## Decision

Usar Apache Kafka (Confluent Platform OSS) como barramento central de eventos entre a camada de CDC (Debezium) e os consumidores downstream (S3 Sink → Databricks Auto Loader).

## Consequences

**Positive:**
- Desacoplamento total entre produtores e consumidores — os consumidores downstream podem ser reiniciados ou ficar offline por até 7 dias (retenção) sem perda de dados.
- Retenção configurável (padrão 7 dias) permite replay gratuito.
- Schema Registry garante contrato de dados entre times via Avro.
- Escalabilidade horizontal via particionamento por driver.

**Negative:**
- Adiciona complexidade operacional (Zookeeper / KRaft, monitoramento JMX).
- Latência de end-to-end ligeiramente maior que gRPC puro.

## Alternatives Considered

| Alternativa | Motivo da recusa |
|---|---|
| AWS Kinesis | Vendor lock-in; custo em escala local |
| RabbitMQ | Sem retenção de mensagens; não escala para streaming |
| Redis Streams | Não adequado para grandes volumes com replay durável |
