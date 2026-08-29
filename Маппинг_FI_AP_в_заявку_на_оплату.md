# Маппинг: открытая позиция кредитора FI-AP → заявка на оплату

**Источник:** `Open Vendor Item (FI-AP) / Открытая позиция кредитора`  
**Получатель:** `Payment Request / Заявка на оплату` в TRM  
**Кардинальность:** одна открытая позиция, доступная к оплате → одна заявка (`1 → 1`).

## 1. маппинг реквизитов

| Реквизит Открытой позиции кредитора (система FI-AP) | техническое наименование в FI-AP | Реквизит заявки на оплату(система TRM) | техническое наименование в TRM | Правило преобразования |
|---|---|---|---|---|
| Кредитор (`Vendor`) | `counterpartyId` | Контрагент (`Counterparty`) | `counterpartyId` | Технический идентификатор кредитора `counterpartyId` передаётся в реквизит `Counterparty`; роль контрагента фиксирована как `VENDOR`. |
| Сумма (`Amount`) | `amount` | Сумма (`Amount`) | `amount` | Значение `amount` передаётся без преобразования; сумма должна быть больше нуля. |
| Валюта (`Currency`) | `currency` | Валюта (`Currency`) | `currency` | Код валюты ISO 4217 передаётся без преобразования. |
| Срок оплаты (`Due date`) | `dueDate` | Срок оплаты (`Due date`) | `dueDate` | Рассчитанная в FI дата оплаты передаётся без преобразования. |
| Код компании (`BUKRS`) | `companyCode` | Номер FI-позиции: код компании (`BUKRS`) | `numberFiPosition.companyCode` | Передаётся без преобразования в реквизит кода компании таблицы `numberFiPosition`. |
| Финансовый год (`GJAHR`) | `fiscalYear` | Номер FI-позиции: финансовый год (`GJAHR`) | `numberFiPosition.fiscalYear` | Передаётся без преобразования в реквизит финансового года таблицы `numberFiPosition`. |
| Группа реквизитов документа источника | `companyCode`, `documentType`, `documentNumber`, `documentData` | Документы: исходный документ | `numberFiPosition.sourceDocumentId → Documents.id` | FI-AP формирует и передаёт `documentKey` из `companyCode`, `documentType`, `documentNumber`, `documentData`. TRM ищет запись `Documents` по `documentKey`. Если запись отсутствует, TRM создаёт её, генерирует внутренний UUID `Documents.id` и сохраняет его в `numberFiPosition.sourceDocumentId`. |
| Документ-основание (`Origin document`) | `originDocumentId → OriginDocument.id` | Документы: документ-основание | `PaymentRequest.originDocumentId → Documents.id` | Данные `companyCode`, `documentType`, `documentNumber`, `documentData` берутся из отдельной таблицы `OriginDocument` по `originDocumentId`. FI-AP формирует и передаёт по ним `documentKey`. TRM ищет `Documents` по `documentKey`. Если запись отсутствует, TRM создаёт её, генерирует внутренний UUID `Documents.id` и сохраняет его в `PaymentRequest.originDocumentId`. Для прямой FI-проводки документ не передаётся, а `originDocumentId` не заполняется. |
| Назначение платежа (`paymentPurpose`) | `paymentPurpose` | Назначение платежа (`paymentPurpose`) | `paymentPurpose` | Строковое значение передаётся без преобразования. |
| Статус (`Status`) | `status` | Статус (`Status`) | `PaymentRequest.status → PaymentRequestStatuses.code` | По строковому значению `status` TRM ищет запись в справочнике `PaymentRequestStatuses` и сохраняет в заявке ссылку на найденную запись. |
| Номер FI-позиции | `positionNumber` | Номер FI-позиции: номер строки | `numberFiPosition.positionNumber` | Передаётся без преобразования в реквизит номера позиции таблицы `numberFiPosition`. |
| Идентификатор FI-позиции (`lineItemID`) | `lineItemID` | Номер FI-позиции: канонический идентификатор | `numberFiPosition.lineItemID` | `lineItemID` и сопутствующие реквизиты `companyCode`, `fiscalYear`, `positionNumber` передаются в составе структуры `numberFiPosition`. Ссылка `sourceDocumentId` присваивается TRM после обработки исходного FI-документа. На этой основе создаётся строка `numberFiPosition`; `paymentRequestId` присваивается при создании заявки. |
| Тип платежа (`Payment method`) | `paymentMethod` | Тип платежа (`Payment method`) | `PaymentRequest.paymentMethod → PaymentMethods.code` | По строковому значению `paymentMethod` TRM ищет запись в справочнике `PaymentMethods` и сохраняет в заявке ссылку на найденную запись. |
| Блокировка платежа (`Payment block`) | `paymentBlock` | Блокировка платежа (`Payment block`) | `paymentBlock` | Логический признак: `true` — платёж заблокирован, `false` — блокировка отсутствует. |

## 2. Технические реквизиты Kafka

Реквизиты этого раздела не входят в бизнес-маппинг заявки. Они передаются в Kafka-событии
и сохраняются в TRM для трассировки и идемпотентной обработки.

| Реквизит Kafka-события | Техническое наименование в FI-AP / Kafka | Реквизит TRM | Техническое наименование в TRM | Правило обработки |
|---|---|---|---|---|
| Идентификатор события | `eventId` | Идентификатор события | `PaymentRequest.eventId` | Передаётся и сохраняется без преобразования; используется для дедупликации сообщения. |
| Идентификатор исходной FI-позиции | `sourceLineItemId` | Идентификатор исходной FI-позиции | `PaymentRequest.sourceLineItemId` | Передаётся и сохраняется без преобразования; используется для трассировки заявки к источнику. |
| Техническая ссылка на исходную FI-позицию | `sourceLineItemReference` | Техническая ссылка на исходную FI-позицию | `PaymentRequest.sourceLineItemReference` | Передаётся и сохраняется без преобразования; используется для открытия позиции в системе-источнике. |



