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
| Код компании (`BUKRS`) | `companyCode` | Код компании (`BUKRS`) | `companyCode` | Передаётся внутрикорпоративный код компании. |
| Документ-основание (`Origin document`) | `originDocument` | Документ-основание (`Origin document`) | `originDocument` | Передаётся структура: `companyCode` — код компании (`BUKRS`), `documentType` — тип документа, `documentNumber` — номер документа и `documentData` — дата документа в формате `YYYY-MM-DD`. |
| Назначение платежа (`reference`)  | `paymentPurpose` | Назначение платежа (`reference`) | `paymentPurpose` | Строковое значение передаётся без преобразования. |
| Статус (`Status`) | `status` | Статус (`Status`) | `status` | При создании заявки TRM устанавливает значение `Created`; изменения после создания должны передаваться отдельным событием `payment.status`. |
| Номер FI-позиции (`number FI position`) | `numberFiPosition` | Номер FI-позиции (`number FI position`) | `numberFiPosition` | Передаётся структурой: `companyCode` (`BUKRS`), `documentType` (`BLART`), `documentNumber`, `documentData`, `fiscalYear` (`GJAHR`) и `positionNumber` (номер FI-позиции). |
| Тип платежа (`Payment method`) | `paymentMethod` | Тип платежа (`Payment method`) | `paymentMethod` | Код способа платежа определяется по единому классификатору типов платежа. |
| Блокировка платежа (`Payment block`) | `paymentBlock` | Блокировка платежа (`Payment block`) | `paymentBlock` | Логический признак: `true` — платёж заблокирован, `false` — блокировка отсутствует. |



