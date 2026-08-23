# Event Contract: FI-AP → Payment Request

**Статус:** Предложение 0.1  
**Топик:** `payment.demand`  
**Producer:** FI, `FI Demand Producer / Outbox`  
**Consumer:** TRM, `Demand Consumer` (`trm-demand`)  
**Сценарий:** `Open Vendor Item (FI-AP) / Открытая позиция кредитора` → `Payment Request / Заявка на оплату`.

## 1. Назначение и границы

Событие сообщает TRM о создании открытой позиции кредитора в FI, по которой возникла
платёжная потребность. TRM создаёт одну заявку на оплату для одной FI-AP позиции (`1 → 1`).

Контракт описывает только создание заявки. Изменение статуса, блокировка, разблокировка
и отмена после её создания должны передаваться отдельным событием статуса
(`payment.status`) после утверждения его контракта.

## 2. Kafka key и идентификаторы

Kafka key и `sourceLineItemId` совпадают и формируются по правилу:

```text
FI|role=VENDOR|CP=<VENDOR_ID>|DOCTYPE=VENDOR_OPEN_ITEM|
DOCNO=<ACCOUNTING_DOCUMENT_NUMBER>-<LINE_ITEM_NUMBER>|
BUKRS=<COMPANY_CODE>|GJAHR=<FISCAL_YEAR>
```

Значение `DOCNO` для FI-AP является составным идентификатором позиции, а не только
номером FI-документа. Это исключает коллизию нескольких позиций одного документа.

| Реквизит | Правило |
|---|---|
| `eventId` | UUID записи Outbox. Не изменяется при retry той же записи. |
| `messageKey` | Kafka key; определяет партицию и порядок событий одной FI-потребности. |
| `sourceLineItemId` | Равен `messageKey`; сохраняется в заявке для трассировки. |
| `correlationId` | Сквозной ID операции FI → Kafka → TRM. Правило генерации требует согласования. |

## 3. Структура события

### 3.1. Метаданные

| Поле | Тип | Обяз. | Значение / правило |
|---|---|:---:|---|
| `eventId` | UUID string | Да | Идентификатор экземпляра публикации. |
| `eventType` | string | Да | Фиксированное значение `payment.demand`. |
| `eventVersion` | string | Да | Версия контракта, например `1.0`. |
| `occurredAt` | datetime | Да | Время возникновения FI-AP позиции. |
| `publishedAt` | datetime | Да | Время публикации записи Outbox. |
| `correlationId` | string | Да | Сквозной идентификатор операции. |
| `producerSystem` | string | Да | Идентификатор SAP FI / Publisher. |

### 3.2. Источник — открытая позиция кредитора

| Поле | Тип | Обяз. | Источник в FI / правило |
|---|---|:---:|---|
| `documentType` | string | Да | Фиксированное значение `VENDOR_OPEN_ITEM`. |
| `companyCode` | string | Да | Код компании `BUKRS`. |
| `fiscalYear` | string | Да | Финансовый год `GJAHR`. |
| `accountingDocumentType` | string | Да | Тип FI-документа `BLART`. |
| `accountingDocumentNumber` | string | Да | Номер FI-документа. |
| `numberFiPosition` | object | Да | Структура номера FI-позиции из раздела 3.3. |
| `lineItemID` | string | Да | Каноническая строка, сформированная из `numberFiPosition`. |
| `sourceLineItemId` | string | Да | Составной ID исходной FI-позиции из раздела 2. |
| `sourceLineItemReference` | string | Да | Техническая ссылка для открытия FI-позиции; формат требует согласования. |

### 3.3. Номер FI-позиции

`numberFiPosition` передаётся как структура:

| Поле | Тип | Обяз. | Источник в FI |
|---|---|:---:|---|
| `numberFiPosition.companyCode` | string | Да | Код компании `BUKRS`. |
| `numberFiPosition.documentType` | string | Да | Тип FI-документа `BLART`. |
| `numberFiPosition.documentNumber` | string | Да | Номер FI-документа. |
| `numberFiPosition.documentData` | date | Да | Дата FI-документа в формате `YYYY-MM-DD`. |
| `numberFiPosition.fiscalYear` | string | Да | Финансовый год `GJAHR`. |
| `numberFiPosition.positionNumber` | string | Да | Номер FI-позиции. |

`lineItemID` является каноническим строковым представлением этой структуры:

```text
FI_POSITION|BUKRS=<COMPANY_CODE>|BLART=<DOCUMENT_TYPE>|
DOCNO=<DOCUMENT_NUMBER>|DOCDATA=<DOCUMENT_DATA>|
GJAHR=<FISCAL_YEAR>|POSNO=<POSITION_NUMBER>
```

`accountingDocumentNumber` также передаётся отдельным реквизитом для прямого поиска
FI-документа; его значение совпадает с `numberFiPosition.documentNumber`.

### 3.4. Документ-основание

Блок передаётся, если открытая позиция создана из внешнего документа, например MIRO.
Для прямой FI-проводки он не заполняется.

| Поле | Тип | Обяз. | Правило |
|---|---|:---:|---|
| `originDocument.companyCode` | string | Условно | Код компании `BUKRS`. |
| `originDocument.documentType` | string | Условно | Тип документа-основания. |
| `originDocument.documentNumber` | string | Условно | Номер документа-основания. |
| `originDocument.documentData` | date | Условно | Дата документа-основания в формате `YYYY-MM-DD`. |

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

## 4. Создание заявки в TRM

| Реквизит Payment Request | Поле события |
|---|---|
| `Amount` | `amount` |
| `Currency` | `currency` |
| `Counterparty` | `counterpartyId` |
| `Due date` | `dueDate` |
| `Company code (BUKRS)` | `companyCode` |
| `originDocument` | `originDocument` |
| `Payment purpose` | `paymentPurpose` |
| `Number FI position` | `numberFiPosition` |
| `Payment method` | `paymentMethod` |
| `Payment block` | `paymentBlock` |
| `Status` | `Created` при создании заявки в TRM. |

TRM должен сохранять `eventId`, `sourceLineItemId` и `sourceLineItemReference` как
технические реквизиты интеграции, даже если они не отображаются в пользовательской форме
заявки.

## 5. Пример payload

```json
{
  "eventId": "0f8fad5b-d9cb-469f-a165-70867728950e",
  "eventType": "payment.demand",
  "eventVersion": "1.0",
  "occurredAt": "2026-08-12T10:15:00Z",
  "publishedAt": "2026-08-12T10:15:02Z",
  "correlationId": "TBD",
  "producerSystem": "SAP_FI",
  "documentType": "VENDOR_OPEN_ITEM",
  "companyCode": "1000",
  "fiscalYear": "2026",
  "accountingDocumentType": "KR",
  "accountingDocumentNumber": "1900012345",
  "numberFiPosition": {
    "companyCode": "1000",
    "documentType": "KR",
    "documentNumber": "1900012345",
    "documentData": "2026-08-12",
    "fiscalYear": "2026",
    "positionNumber": "001"
  },
  "lineItemID": "FI_POSITION|BUKRS=1000|BLART=KR|DOCNO=1900012345|DOCDATA=2026-08-12|GJAHR=2026|POSNO=001",
  "sourceLineItemId": "FI|role=VENDOR|CP=0001007788|DOCTYPE=VENDOR_OPEN_ITEM|DOCNO=1900012345-001|BUKRS=1000|GJAHR=2026",
  "sourceLineItemReference": "TBD",
  "originDocument": {
    "companyCode": "1000",
    "documentType": "MIRO",
    "documentNumber": "5100001234",
    "documentData": "2026-08-12"
  },
  "counterpartyId": "0001007788",
  "counterpartyRole": "VENDOR",
  "amount": 125000.00,
  "currency": "RUB",
  "dueDate": "2026-08-20",
  "paymentPurpose": "Оплата по счёту поставщика",
  "paymentMethod": "BANK_TRANSFER",
  "paymentBlock": false
}
```

## 6. Валидация и обработка

1. Publisher не публикует событие без `BUKRS`, `GJAHR`, номера FI-документа,
   `numberFiPosition`, кредитора, суммы, валюты или срока оплаты.
2. `amount` должен быть больше нуля, а `currency` — соответствовать ISO 4217.
3. TRM дедуплицирует сообщения по `eventId`; повторное сообщение не создаёт вторую заявку.
4. Неизвестные коды `paymentMethod` направляются на разбор как ошибка классификатора.
5. Ошибки валидации и исчерпанные временные retry направляются в `payment.demand.DLQ`.

## 7. Открытые решения

1. Формат `correlationId` и `sourceLineItemReference`.
2. Версия UUID для `eventId`.
3. Полный состав и владельцы единого классификатора типов платежа.
4. Формат хранения и отображения технических реквизитов интеграции в TRM.
5. Контракт `payment.status` для синхронизации изменений статуса и блокировки после создания заявки.
6. Согласование составного формата `DOCNO` для FI-AP с общей спецификацией ключей.
