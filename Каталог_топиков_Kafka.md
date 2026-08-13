# Каталог Kafka-топиков

| Topic | Назначение | Producer | Consumer | Partitions | Retention |
|---|---|---|---|---:|---:|
| `payment.demand` | Исходящие платёжные потребности из FI для создания заявок в TRM | FI Demand Producer / Outbox | TRM Demand Consumer (`trm-demand`) | 4 | 7 |
| `payment.demand.DLQ` | События `payment.demand`, не обработанные в TRM после исчерпания retry | TRM Demand Consumer | Команда поддержки / процесс reprocess | 1 | 30 |
| `payment.status` | Изменения статуса заявки на оплату из TRM для обновления FI | TRM Status Publisher | FI Status Consumer | 2 | 7 |
| `payment.status.DLQ` | События `payment.status`, не обработанные в FI после исчерпания retry | FI Status Consumer | Команда поддержки / процесс reprocess | 1 | 30 |

## Примечания

- Для `payment.demand` количество партиций зафиксировано: 4.
- Значения `Retention` и число партиций для DLQ требуют согласования с владельцем Kafka-платформы.
- Топики `payment.status` и `payment.status.DLQ` являются предлагаемыми: для них необходимо отдельно утвердить event contract, допустимые статусы и правила обновления FI.
- Отдельный retry-топик в текущей схеме не предусмотрен: повторные попытки выполняет Consumer, после чего сообщение направляется в соответствующий DLQ.
