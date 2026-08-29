# Event Contract: FI-AP → Payment Request

**Статус:** Предложение 0.1  
**Топик:** `payment.demand`  
**Producer:** FI, `FI Demand Producer / Outbox`  
**Consumer:** TRM, `Demand Consumer` (`trm-demand`)  
**Сценарий:** `Open Vendor Item (FI-AP) / Открытая позиция кредитора` → `Payment Request / Заявка на оплату`.

## 1. Назначение и границы

Событие сообщает TRM о создании или изменении открытой позиции кредитора в FI,
по которой возникла платёжная потребность. Одной FI-AP позиции соответствует одна
заявка на оплату в TRM (`1 → 1`).

TRM обрабатывает событие как идемпотентный upsert по `sourceLineItemId`: создаёт заявку,
если она отсутствует, либо актуализирует существующую заявку при получении нового
`eventId`.

`payment.demand` передаёт актуальное состояние объекта со стороны FI-AP. Изменения,
возникшие на стороне TRM и предназначенные для обратной синхронизации с FI,
передаются отдельным событием `payment.status` после утверждения его контракта.

## 2. Kafka key и идентификаторы

Kafka key и `sourceLineItemId` совпадают и формируются по правилу:

```text
FI|role=VENDOR|CP=<VENDOR_ID>|DOCTYPE=VENDOR_OPEN_ITEM|
DOCNO=<DOCUMENT_NUMBER>-<POSITION_NUMBER>|
BUKRS=<COMPANY_CODE>|GJAHR=<FISCAL_YEAR>
```

Значение `DOCNO` для FI-AP является составным идентификатором позиции, а не только
номером FI-документа. Это исключает коллизию нескольких позиций одного документа.

| Реквизит | Правило |
|---|---|
| `eventId` | UUID записи Outbox. Не изменяется при retry той же записи. |
| `messageKey` | Kafka key; определяет партицию и порядок событий одной FI-потребности. |
| `sourceLineItemId` | Равен `messageKey`; сохраняется в заявке для трассировки. |

## 3. Структура события

### 3.1. Метаданные

| Поле | Тип | Обяз. | Значение / правило |
|---|---|:---:|---|
| `eventId` | UUID string | Да | Идентификатор экземпляра публикации. |
| `eventType` | string | Да | Фиксированное значение `payment.demand`. |
| `eventVersion` | string | Да | Версия контракта, например `1.0`. |
| `occurredAt` | datetime | Да | Время возникновения FI-AP позиции. |
| `publishedAt` | datetime | Да | Время публикации записи Outbox. |
| `producerSystem` | string | Да | Идентификатор SAP FI / Publisher. |

### 3.2. Источник — открытая позиция кредитора

| Поле | Тип | Обяз. | Источник в FI / правило |
|---|---|:---:|---|
| `documentType` | string | Да | Тип сущности-источника. Фиксированное значение `VENDOR_OPEN_ITEM`. |
| `numberFiPosition` | object | Да | Структура FI-позиции из раздела 3.3. Формируется FI-AP из реквизитов открытой позиции. |
| `originDocument` | object | Условно | Документ-основание из раздела 3.4. Для прямой FI-проводки не передаётся. |
| `sourceLineItemId` | string | Да | Составной идентификатор исходной FI-позиции. Совпадает с Kafka key. |
| `sourceLineItemReference` | string | Да | Техническая ссылка для открытия FI-позиции в системе-источнике; формат требует согласования. |

### 3.3. Номер FI-позиции

`numberFiPosition` передаётся как структура:

| Поле | Тип | Обяз. | Источник в FI |
|---|---|:---:|---|
| `numberFiPosition.companyCode` | string | Да | Код компании `BUKRS`. |
| `numberFiPosition.fiscalYear` | string | Да | Финансовый год `GJAHR`. |
| `numberFiPosition.positionNumber` | string | Да | Номер FI-позиции. |
| `numberFiPosition.lineItemID` | string | Да | Канонический идентификатор FI-позиции, сформированный FI-AP. |
| `numberFiPosition.sourceDocument` | object | Да | Реквизиты исходного документа для поиска или создания записи `Documents` в TRM. |

`numberFiPosition.sourceDocument` содержит:

| Поле | Тип | Обяз. | Источник в FI / правило |
|---|---|:---:|---|
| `numberFiPosition.sourceDocument.companyCode` | string | Да | Код компании; должен совпадать с `numberFiPosition.companyCode`. |
| `numberFiPosition.sourceDocument.documentType` | string | Да | Фиксированное значение `VENDOR_OPEN_ITEM`. |
| `numberFiPosition.sourceDocument.documentNumber` | string | Да | Номер исходного документа. |
| `numberFiPosition.sourceDocument.documentData` | date | Да | Дата исходного документа в формате `YYYY-MM-DD`. |
| `numberFiPosition.sourceDocument.documentKey` | string | Да | Канонический ключ документа, сформированный FI-AP. |

`documentKey` формируется по правилу:

```text
DOCUMENT|BUKRS=<COMPANY_CODE>|DOCTYPE=<DOCUMENT_TYPE>|
DOCNO=<DOCUMENT_NUMBER>|DOCDATA=<DOCUMENT_DATA>
```

FI-AP формирует и передаёт `lineItemID`; TRM проверяет его соответствие остальным
реквизитам структуры. Формат `lineItemID`:

```text
FI_POSITION|BUKRS=<COMPANY_CODE>|DOCTYPE=<DOCUMENT_TYPE>|
DOCNO=<DOCUMENT_NUMBER>|DOCDATA=<DOCUMENT_DATA>|
GJAHR=<FISCAL_YEAR>|POSNO=<POSITION_NUMBER>
```

TRM ищет `Documents` по `documentKey`. Если запись отсутствует, TRM создаёт её,
генерирует внутренний UUID `Documents.id` и сохраняет его в
`numberFiPosition.sourceDocumentId`.

### 3.4. Документ-основание

Блок передаётся, если открытая позиция создана из внешнего документа, например MIRO.
Для прямой FI-проводки он не заполняется.

| Поле | Тип | Обяз. | Правило |
|---|---|:---:|---|
| `originDocument.companyCode` | string | Условно | Код компании `BUKRS`. |
| `originDocument.documentType` | string | Условно | Тип документа-основания. |
| `originDocument.documentNumber` | string | Условно | Номер документа-основания. |
| `originDocument.documentData` | date | Условно | Дата документа-основания в формате `YYYY-MM-DD`. |
| `originDocument.documentKey` | string | Условно | Канонический ключ документа-основания, сформированный FI-AP. |

`originDocument.documentKey` формируется по правилу:

```text
DOCUMENT|BUKRS=<COMPANY_CODE>|DOCTYPE=<DOCUMENT_TYPE>|
DOCNO=<DOCUMENT_NUMBER>|DOCDATA=<DOCUMENT_DATA>
```

TRM ищет `Documents` по `originDocument.documentKey`. Если запись отсутствует,
TRM создаёт её, генерирует внутренний UUID `Documents.id` и сохраняет его в
`PaymentRequest.originDocumentId`.

### 3.5. Данные платёжной потребности

| Поле | Тип | Обяз. | Маппинг в Payment Request / правило |
|---|---|:---:|---|
| `counterpartyId` | string | Да | → `Counterparty`; технический ID кредитора. |
| `counterpartyRole` | string | Да | Фиксированное значение `VENDOR`. |
| `amount` | decimal | Да | → `Amount`; строго больше нуля. |
| `currency` | string | Да | → `Currency`; трёхбуквенный код ISO 4217. |
| `dueDate` | date | Да | → `Due date`; рассчитанная в FI дата оплаты. |
| `paymentPurpose` | string | Да | → `Payment purpose`. |
| `paymentMethod` | string | Да | → `Payment method`; код из единого классификатора типов платежа. |
| `paymentBlock` | boolean | Да | → `Payment block`; `true` — платёж заблокирован, `false` — блокировка отсутствует. |
| `status` | string | Да | По значению статуса источника TRM ищет запись в справочнике `PaymentRequestStatuses`. |

## 4. Создание заявки в TRM

### 4.1. Заявка на оплату

| Реквизит TRM | Поле события | Правило |
|---|---|---|
| `PaymentRequest.id` | — | UUID генерируется TRM. |
| `PaymentRequest.counterpartyId` | `counterpartyId` | TRM находит контрагента по `Counterparties.id`. |
| `PaymentRequest.amount` | `amount` | Передаётся без преобразования. |
| `PaymentRequest.currency` | `currency` | Передаётся без преобразования. |
| `PaymentRequest.dueDate` | `dueDate` | Передаётся без преобразования. |
| `PaymentRequest.paymentPurpose` | `paymentPurpose` | Передаётся без преобразования. |
| `PaymentRequest.paymentBlock` | `paymentBlock` | Передаётся без преобразования. |
| `PaymentRequest.paymentMethod` | `paymentMethod` | TRM ищет запись в `PaymentMethods` по переданному коду. |
| `PaymentRequest.status` | `status` | TRM ищет запись в `PaymentRequestStatuses` по переданному коду. |
| `PaymentRequest.originDocumentId` | `originDocument.documentKey` | TRM находит или создаёт `Documents`. Для прямой FI-проводки поле не заполняется. |

### 4.2. Номер FI-позиции

Для созданной заявки TRM создаёт одну связанную запись `numberFiPosition`.

| Реквизит TRM | Поле события | Правило |
|---|---|---|
| `numberFiPosition.paymentRequestId` | — | Заполняется значением созданного `PaymentRequest.id`. |
| `numberFiPosition.companyCode` | `numberFiPosition.companyCode` | TRM проверяет наличие кода в `CompanyCodes`. |
| `numberFiPosition.fiscalYear` | `numberFiPosition.fiscalYear` | Передаётся без преобразования. |
| `numberFiPosition.positionNumber` | `numberFiPosition.positionNumber` | Передаётся без преобразования. |
| `numberFiPosition.lineItemID` | `numberFiPosition.lineItemID` | Передаётся после проверки канонического формата. |
| `numberFiPosition.sourceDocumentId` | `numberFiPosition.sourceDocument.documentKey` | TRM находит или создаёт `Documents` и сохраняет внутренний `Documents.id`. |

### 4.3. Технические реквизиты

| Реквизит TRM | Поле события |
|---|---|
| `PaymentRequest.eventId` | `eventId` |
| `PaymentRequest.sourceLineItemId` | `sourceLineItemId` |
| `PaymentRequest.sourceLineItemReference` | `sourceLineItemReference` |

## 5. Пример payload

```json
{
  "eventId": "0f8fad5b-d9cb-469f-a165-70867728950e",
  "eventType": "payment.demand",
  "eventVersion": "1.0",
  "occurredAt": "2026-08-12T10:15:00Z",
  "publishedAt": "2026-08-12T10:15:02Z",
  "producerSystem": "SAP_FI",
  "documentType": "VENDOR_OPEN_ITEM",
  "numberFiPosition": {
    "companyCode": "1000",
    "fiscalYear": "2026",
    "positionNumber": "001",
    "lineItemID": "FI_POSITION|BUKRS=1000|DOCTYPE=VENDOR_OPEN_ITEM|DOCNO=1900012345|DOCDATA=2026-08-12|GJAHR=2026|POSNO=001",
    "sourceDocument": {
      "companyCode": "1000",
      "documentType": "VENDOR_OPEN_ITEM",
      "documentNumber": "1900012345",
      "documentData": "2026-08-12",
      "documentKey": "DOCUMENT|BUKRS=1000|DOCTYPE=VENDOR_OPEN_ITEM|DOCNO=1900012345|DOCDATA=2026-08-12"
    }
  },
  "sourceLineItemId": "FI|role=VENDOR|CP=0001007788|DOCTYPE=VENDOR_OPEN_ITEM|DOCNO=1900012345-001|BUKRS=1000|GJAHR=2026",
  "sourceLineItemReference": "TBD",
  "originDocument": {
    "companyCode": "1000",
    "documentType": "MIRO",
    "documentNumber": "5100001234",
    "documentData": "2026-08-12",
    "documentKey": "DOCUMENT|BUKRS=1000|DOCTYPE=MIRO|DOCNO=5100001234|DOCDATA=2026-08-12"
  },
  "counterpartyId": "0001007788",
  "counterpartyRole": "VENDOR",
  "amount": 125000.00,
  "currency": "RUB",
  "dueDate": "2026-08-20",
  "paymentPurpose": "Оплата по счёту поставщика",
  "paymentMethod": "BANK_TRANSFER",
  "paymentBlock": false,
  "status": "CREATED"
}
```

## 6. Валидация и обработка

### 6.1. Проверка обязательных реквизитов

Publisher не публикует событие без обязательных реквизитов: `eventId`,
`sourceLineItemId`, `numberFiPosition`, `counterpartyId`, `amount`, `currency`,
`dueDate`, `paymentMethod` и `status`.

### 6.2. Валидация значений

До сохранения TRM проверяет:

- `amount` больше нуля;
- `currency` соответствует ISO 4217;
- `documentKey` соответствует реквизитам документа;
- `lineItemID` соответствует реквизитам FI-позиции;
- коды контрагента, компании, способа оплаты и статуса существуют
  в соответствующих справочниках.

### 6.3. Повторная доставка события

Если `eventId` уже был обработан, событие считается повторной доставкой.
Данные в TRM не изменяются, Kafka offset фиксируется.

### 6.4. Поиск заявки

Если `eventId` ранее не обрабатывался, TRM выполняет поиск заявки
по `sourceLineItemId`.

### 6.5. Создание заявки

Если заявка с таким `sourceLineItemId` отсутствует, TRM:

- находит или создаёт записи `Documents`;
- создаёт `PaymentRequest`;
- создаёт связанную запись `numberFiPosition`;
- сохраняет `eventId` как обработанный.

### 6.6. Обновление заявки

Если заявка с таким `sourceLineItemId` существует, новый `eventId`
означает изменение объекта в системе-источнике. TRM актуализирует:

- реквизиты `PaymentRequest`;
- связанную запись `numberFiPosition`;
- ссылки на исходный документ и документ-основание;
- сохраняет новый `eventId` как обработанный.

При обновлении существующие `PaymentRequest.id` и
`numberFiPosition.paymentRequestId` не изменяются.

### 6.7. Транзакционность

Создание или обновление `Documents`, `PaymentRequest`, `numberFiPosition`
и регистрация обработанного `eventId` выполняются в одной транзакции.

### 6.8. Фиксация Kafka offset и обработка ошибок

Kafka offset фиксируется только после успешного завершения транзакции.
Ошибки валидации и ошибки после исчерпания retry направляются
в `payment.demand.DLQ`.

Для регистрации всех обработанных `eventId` TRM должен использовать отдельное
хранилище входящих событий. Одного поля `PaymentRequest.eventId` недостаточно:
после обновления необходимо распознавать повторную доставку любого ранее
обработанного события.

## 7. Открытые решения

1. Формат `sourceLineItemReference`.
2. Версия UUID для `eventId`.
3. Полный состав и владельцы единого классификатора типов платежа.
4. Формат хранения и отображения технических реквизитов интеграции в TRM.
5. Контракт `payment.status` для синхронизации изменений статуса и блокировки после создания заявки.
6. Согласование составного формата `DOCNO` для FI-AP с общей спецификацией ключей.
