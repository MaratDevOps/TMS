# FI-AP

Вспомогательный CLI-генератор открытых позиций кредиторов и событий `payment.demand`.
Не является реализацией SAP FI-AP.

Спецификация: [`docs/`](docs/).

```text
go run ./cmd/fi-ap-generator --config configs/generation.yaml --scenario smoke
```

Сервис загружает сценарий, применяет миграции Goose, создаёт позиции и записи Outbox
в PostgreSQL и публикует события в Kafka через обработчик Outbox.

Пока процесс жив, Prometheus scrape слушает `metrics.listen` (по умолчанию `:9090`), путь `/metrics`.

Интеграционный тест `TestGenerateAndPublishPaymentDemand` поднимает PostgreSQL и Kafka через Testcontainers.
Если Docker недоступен, тест пропускается. Принудительно пропустить: `SKIP_ITEST=1`.

PostgreSQL (`postgres.dsn`) и брокеры Kafka (`kafka.brokers`) задаются в
`configs/generation.yaml`.
