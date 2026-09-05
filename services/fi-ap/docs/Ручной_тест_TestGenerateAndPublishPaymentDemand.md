# Ручной тест: TestGenerateAndPublishPaymentDemand

## 1. Назначение

Документ описывает **ручной сквозной прогон**, эквивалентный автоматическому интеграционному тесту `TestGenerateAndPublishPaymentDemand` из `internal/app/integration_test.go`.

**Цель проверки:** убедиться, что сервис FI-AP:

1. создаёт открытые позиции кредиторов и документы-основания в PostgreSQL;
2. формирует записи Outbox в той же транзакции;
3. публикует события `payment.demand` в Kafka через обработчик Outbox;
4. завершает работу с успешным итоговым статусом.

| Элемент | Значение |
|---|---|
| Идентификатор | MT-FI-AP-ITEST-01 |
| Автотест-источник | `TestGenerateAndPublishPaymentDemand` |
| Конфигурация прогона | `configs/itest-manual.yaml` |
| Сценарий | `itest` |
| Ожидаемое число позиций / событий | **3** |

---

## 2. Что проверяет автотест (карта соответствия)

| № | Автоматическая проверка | Ручной этап |
|---:|---|---|
| 1 | PostgreSQL и Kafka доступны, миграции применены | [Этап 0](#этап-0-подготовка-окружения) |
| 2 | `Run()` завершился с `exitCode: 0` | [Этап 1](#этап-1-запуск-генератора) |
| 3 | `createdPositions = 3`, `publishedEvents = 3`, `publicationErrors = 0` | [Этап 2](#этап-2-проверка-yaml-результата) |
| 4 | В БД: 3 строки в `open_vendor_items`, 3 в `origin_documents`, 3 попытки со статусом `PUBLISHED` | [Этап 3](#этап-3-проверка-postgresql) |
| 5 | В Kafka: 3 сообщения в топике `payment.demand` | [Этап 4](#этап-4-проверка-kafka) |
| 6 | У каждого сообщения уникальный непустой key | [Этап 4.2](#этап-42-проверка-ключей-сообщений) |
| 7 | Payload: `eventType = payment.demand`, `sourceLineItemId` совпадает с Kafka key | [Этап 4.3](#этап-43-проверка-тела-сообщения) |
| 8 | Заголовки: `eventId`, `eventType`, `producerSystem = SAP_FI` | [Этап 4.4](#этап-44-проверка-заголовков-kafka) |

---

## 3. Предусловия

Перед началом убедитесь, что доступны:

- **Docker Desktop** — **обязателен** для подготовки окружения на Этапе 0;
- **Go 1.22+** (для запуска CLI);
- **psql** или другой SQL-клиент к PostgreSQL;
- **kafka-console-consumer** / **kcat** / UI (Redpanda Console, AKHQ и т.п.) для чтения топика.

Рабочая директория для всех команд:

```text
services/fi-ap
```

> **Почему Docker?** Автотест `TestGenerateAndPublishPaymentDemand` поднимает PostgreSQL и Kafka через **Testcontainers** — это те же зависимости, но в контейнерах. Для ручного прогона мы воспроизводим то же окружение явно через `docker run`, используя **те же образы**, что и в автотесте:
>
> | Зависимость | Образ в автотесте | Образ в ручном прогоне |
> |---|---|---|
> | PostgreSQL | `postgres:16-alpine` | `postgres:16-alpine` |
> | Kafka | `confluentinc/confluent-local:7.5.0` | `confluentinc/confluent-local:7.5.0` |
>
> Альтернатива — уже развёрнутые на стенде PostgreSQL и Kafka: тогда Docker не нужен, но DSN и `kafka.brokers` в `configs/itest-manual.yaml` нужно указать под ваш стенд.

---

## 4. Этапы выполнения

### Этап 0. Подготовка окружения (Docker)

На этом этапе вы **вручную делаете то, что автотест делает через Testcontainers**: поднимаете PostgreSQL и Kafka в Docker-контейнерах и проверяете их доступность с хоста.

#### 0.0. Проверить, что Docker запущен

```powershell
docker version
docker info
```

**Ожидание:** команды выполняются без ошибки `Cannot connect to the Docker daemon`.

На Windows убедитесь, что **Docker Desktop** запущен (иконка в трее, статус *Running*). Автотест на Windows по умолчанию использует pipe `npipe:////./pipe/dockerDesktopLinuxEngine` — тот же Docker Desktop, что и для ручного прогона.

#### 0.1. Поднять PostgreSQL (Docker)

```powershell
docker run -d --name fiap-postgres `
  -e POSTGRES_USER=fiap `
  -e POSTGRES_PASSWORD=fiap `
  -e POSTGRES_DB=fiap `
  -p 5432:5432 `
  postgres:16-alpine
```

**Ожидаемый результат:** контейнер в статусе `running`, подключение по DSN из конфига успешно.

Проверка:

```powershell
docker exec fiap-postgres pg_isready -U fiap -d fiap
```

#### 0.2. Поднять Kafka (Docker)

```powershell
docker run -d --name fiap-kafka `
  -p 9092:9092 `
  confluentinc/confluent-local:7.5.0
```

**Ожидаемый результат:** брокер доступен на `localhost:9092`.

Проверка, что контейнер работает:

```powershell
docker ps --filter name=fiap-kafka
docker logs fiap-kafka --tail 20
```

> Если порт 9092 занят, измените проброс порта и обновите `kafka.brokers` в `configs/itest-manual.yaml`.

#### 0.3. Проверить оба контейнера перед запуском генератора

```powershell
docker ps --filter name=fiap-
```

**Ожидание:** в списке **два** контейнера — `fiap-postgres` и `fiap-kafka`, оба в статусе `Up`.

| Контейнер | Порт на хосте | Назначение |
|---|---|---|
| `fiap-postgres` | `5432` | БД для позиций, Outbox и миграций |
| `fiap-kafka` | `9092` | Публикация событий `payment.demand` |

#### 0.4. Очистить данные предыдущих прогонов (рекомендуется)

Для «чистого» прогона удалите контейнеры и создайте их заново:

```powershell
docker rm -f fiap-postgres fiap-kafka
```

Затем повторите шаги 0.1–0.2.

> Миграции схемы применяются автоматически при старте генератора. Отдельно выполнять SQL-скрипты не нужно.

#### 0.5. Зафиксировать время начала (для фильтрации в Kafka)

Запишите текущее UTC-время — оно понадобится, если в топике уже есть старые сообщения.

---

### Этап 1. Запуск генератора

Из каталога `services/fi-ap` выполните:

```powershell
go run ./cmd/fi-ap-generator --config configs/itest-manual.yaml --scenario itest 2> run.log
```

Результат сохраните в файл (stdout — YAML, stderr — JSON-логи):

```powershell
go run ./cmd/fi-ap-generator --config configs/itest-manual.yaml --scenario itest 1> result.yaml 2> run.log
echo $LASTEXITCODE
```

**Ожидаемый результат:**

| Проверка | Ожидание |
|---|---|
| Код завершения процесса | `0` |
| В `run.log` нет ошибок `postgres unavailable`, `kafka unavailable`, `generation failed` | да |
| Процесс завершился сам, без `Ctrl+C` | да |

---

### Этап 2. Проверка YAML-результата

Откройте `result.yaml` (или stdout последней команды).

**Ожидаемые значения полей:**

| Поле | Ожидание |
|---|---|
| `status` | `COMPLETED` |
| `exitCode` | `0` |
| `scenario` | `itest` |
| `requestedPositions` | `3` |
| `createdPositions` | `3` |
| `publishedEvents` | `3` |
| `generationErrors` | `0` |
| `publicationErrors` | `0` |
| `parallelThreads` | `2` |
| `errorCodes` | пустой список `[]` |

**Пример фрагмента успешного результата:**

```yaml
status: COMPLETED
exitCode: 0
requestedPositions: 3
createdPositions: 3
publishedEvents: 3
generationErrors: 0
publicationErrors: 0
parallelThreads: 2
errorCodes: []
```

**Критерий прохождения этапа:** все значения совпадают с таблицей.

---

### Этап 3. Проверка PostgreSQL

Подключитесь к БД:

```text
postgres://fiap:fiap@localhost:5432/fiap?sslmode=disable
```

#### 3.1. Количество открытых позиций

```sql
SELECT COUNT(*) AS open_vendor_items_count FROM open_vendor_items;
```

**Ожидание:** `3`

#### 3.2. Количество документов-оснований

```sql
SELECT COUNT(*) AS origin_documents_count FROM origin_documents;
```

**Ожидание:** `3`

> В сценарии `itest` параметр `originDocumentShare: 1.0`, поэтому у каждой позиции есть документ-основание типа `MIRO`.

#### 3.3. Статусы публикации Outbox

```sql
SELECT COUNT(*) AS published_attempts_count
FROM outbox_delivery_attempts
WHERE status = 'PUBLISHED';
```

**Ожидание:** `3`

#### 3.4. Связность данных (выборочно)

```sql
SELECT
    ovi.source_line_item_id,
    o.event_id,
    o.message_key,
    oda.status,
    oda.partition,
    oda.offset_value
FROM open_vendor_items ovi
JOIN outbox o ON o.aggregate_id = ovi.id
LEFT JOIN LATERAL (
    SELECT status, partition, offset_value
    FROM outbox_delivery_attempts
    WHERE event_id = o.event_id
    ORDER BY attempt_number DESC
    LIMIT 1
) oda ON TRUE
ORDER BY ovi.source_line_item_id;
```

**Ожидание для каждой из 3 строк:**

| Поле | Ожидание |
|---|---|
| `message_key` | равен `source_line_item_id` |
| `status` | `PUBLISHED` |
| `partition` | не `NULL`, `>= 0` |
| `offset_value` | не `NULL`, `>= 0` |

#### 3.5. Содержимое Outbox payload (выборочно)

```sql
SELECT
    message_key,
    payload->>'eventType' AS event_type,
    payload->>'sourceLineItemId' AS source_line_item_id,
    payload->>'producerSystem' AS producer_system
FROM outbox
ORDER BY created_at;
```

**Ожидание для каждой записи:**

| Поле | Ожидание |
|---|---|
| `event_type` | `payment.demand` |
| `source_line_item_id` | совпадает с `message_key` |
| `producer_system` | `SAP_FI` |

**Критерий прохождения этапа:** все SQL-проверки дают ожидаемые значения, связи между таблицами корректны.

---

### Этап 4. Проверка Kafka

Топик: **`payment.demand`**

#### 4.1. Получить сообщения из топика

**Вариант A — через контейнер Confluent:**

```powershell
docker exec fiap-kafka kafka-console-consumer `
  --bootstrap-server localhost:9092 `
  --topic payment.demand `
  --from-beginning `
  --timeout-ms 10000 `
  --property print.key=true `
  --property print.headers=true `
  --property key.separator=" | "
```

**Вариант B — kcat (если установлен):**

```powershell
kcat -b localhost:9092 -t payment.demand -C -f "key=%k headers=%h payload=%s`n" -o beginning -e
```

> Если в топике есть сообщения от предыдущих прогонов, отберите **3 сообщения текущего запуска** по времени или по `runId` из `result.yaml` (через SQL можно получить список `message_key`).

Получить ключи текущего прогона из БД:

```sql
SELECT message_key
FROM outbox
ORDER BY created_at DESC
LIMIT 3;
```

**Ожидание:** найдено **ровно 3** сообщения с ключами из этого списка.

#### 4.2. Проверка ключей сообщений

Для каждого из 3 сообщений:

| Проверка | Ожидание |
|---|---|
| Kafka key не пустой | да |
| Все 3 ключа уникальны | да |
| Key совпадает с `sourceLineItemId` в payload | да |

#### 4.3. Проверка тела сообщения

Распарсьте JSON payload каждого сообщения.

**Обязательные поля и значения:**

| Поле JSON | Ожидание |
|---|---|
| `eventType` | `payment.demand` |
| `eventVersion` | `1.0` |
| `producerSystem` | `SAP_FI` |
| `documentType` | `VENDOR_OPEN_ITEM` |
| `sourceLineItemId` | равен Kafka key |
| `counterpartyId` | `0001007788` |
| `counterpartyRole` | `VENDOR` |
| `currency` | `RUB` |
| `paymentMethod` | `BANK_TRANSFER` |
| `status` | `CREATED` |
| `originDocument.documentType` | `MIRO` |
| `numberFiPosition.companyCode` | `1000` |

Дополнительно:

- `eventId` — валидный UUID;
- `amount` — число в диапазоне **100.00–1000.00** (по параметрам сценария);
- `paymentBlock` — `false` (вероятность блокировки в сценарии = 0).

#### 4.4. Проверка заголовков Kafka

Для каждого сообщения проверьте заголовки:

| Заголовок | Ожидание |
|---|---|
| `eventId` | равен полю `eventId` в JSON payload |
| `eventType` | `payment.demand` |
| `producerSystem` | `SAP_FI` |

**Критерий прохождения этапа:** 3 сообщения найдены, ключи уникальны, payload и заголовки соответствуют контракту.

---

## 5. Итоговый чек-лист тестировщика

Отметьте каждый пункт после выполнения:

- [ ] Docker Desktop запущен, `docker version` без ошибок
- [ ] Контейнеры `fiap-postgres` и `fiap-kafka` в статусе `Up`
- [ ] PostgreSQL доступен, схема создана
- [ ] Kafka доступна, топик `payment.demand` принимает сообщения
- [ ] Генератор завершился с кодом `0`
- [ ] YAML: `status=COMPLETED`, `createdPositions=3`, `publishedEvents=3`, ошибок нет
- [ ] SQL: `open_vendor_items` = 3
- [ ] SQL: `origin_documents` = 3
- [ ] SQL: `outbox_delivery_attempts` со статусом `PUBLISHED` = 3
- [ ] Kafka: получено 3 сообщения текущего прогона
- [ ] Kafka: 3 уникальных непустых key
- [ ] Kafka: `sourceLineItemId` в payload = key
- [ ] Kafka: заголовки `eventId`, `eventType`, `producerSystem` корректны

**Тест пройден**, если все пункты отмечены.

---

## 6. Негативные сценарии (опционально)

Эти проверки **не входят** в автотест, но полезны для ручной диагностики:

| Действие | Ожидаемое поведение |
|---|---|
| Остановить Kafka перед запуском | `exitCode: 3`, `status: FAILED`, код `DEPENDENCY_UNAVAILABLE` |
| Остановить PostgreSQL перед запуском | `exitCode: 3`, `status: FAILED`, код `DEPENDENCY_UNAVAILABLE` |
| Указать несуществующий сценарий | `exitCode: 2`, код `CONFIGURATION_ERROR` |

---

## 7. Устранение типовых проблем

| Симптом | Возможная причина | Действие |
|---|---|---|
| `exitCode: 3` | PostgreSQL или Kafka недоступны | Проверить контейнеры, порты, DSN и `kafka.brokers` |
| `createdPositions < 3` | Ошибка генерации / конфликт данных | Смотреть `run.log`, очистить БД и повторить |
| `publishedEvents < 3` | Outbox не успел опубликовать | Увеличить ожидание или проверить Kafka; смотреть `outbox_delivery_attempts` |
| В Kafka больше 3 сообщений | Старые данные в топике | Фильтровать по ключам из SQL или поднять чистый Kafka |
| Порт 5432/9092 занят | Конфликт с локальными сервисами | Сменить порты в Docker и конфиге |

---

## 8. Сверка с автотестом (для QA / разработки)

Чтобы убедиться, что ручная инструкция актуальна, можно запустить автотест:

```powershell
cd services/fi-ap
go test ./internal/app -run TestGenerateAndPublishPaymentDemand -count=1 -v
```

**Ожидание:** тест `PASS` (при доступном Docker).

Принудительный пропуск:

```powershell
$env:SKIP_ITEST = "1"
go test ./internal/app -run TestGenerateAndPublishPaymentDemand -count=1 -v
```

---

## 9. Связанные документы

- [`Сценарий_запуска_генератора.md`](Сценарий_запуска_генератора.md) — общий пользовательский сценарий CLI;
- [`Генерация_открытых_позиций.md`](Генерация_открытых_позиций.md) — параметры сценариев;
- [`Алгоритм_обработчика_Outbox.md`](Алгоритм_обработчика_Outbox.md) — логика публикации;
- [`../configs/itest-manual.yaml`](../configs/itest-manual.yaml) — конфигурация ручного прогона;
- [`../testdata/payment.demand.golden.json`](../testdata/payment.demand.golden.json) — пример структуры payload.
