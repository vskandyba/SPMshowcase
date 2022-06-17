### Инструкция по запуску

git clone https://github.com/vskandyba/SPMshowcase.git

cd SPMshowcase/release/

chmod +x start.sh start-alternative.sh

./start.sh — ParquetCreator -> CorporatePaymentBuilder -> CorporateAccountBuilder -> CorporateInfoBuilder -> DBSender

./start-alternative.sh — ParquetCreator -> DataMartsBuilder -> DBSender

---
### Структура проекта

config/calculation_params_tech — технологическая таблица

config/settings.json — основные настройки проекта

resources/input/ — поступающие данные

resources/output/ — данные в результате партицирования в формате parquet

resources/output/DataMartFromSpark/ — витрины в формате parquet (spark api)

resources/output/DataMartFromSQL/ — витрины в формате parquet (spark sql)

resources/SQL/ — sql-запросы для создания витрин

resources/debug/ — представление витрин в формате csv (отсортированы)

---
### Основные классы

ParquetCreator — преобразование файлов входных csv в parquet

WriteSaveUtilities — функции, которые используются другими классами

CorporatePaymentBuilder — построитель витрины CorporatePayment

CorporateAccountBuilder — построитель витрины CorporateAccount

CorporateInfoBuilder — построитель витрины CorporateInfo

DataMartsBuilder — построитель сразу трёх витрин через sql: CorporatePayment, CorporateAccount, CorporateInfo

DBSender — читает сформированные витрины из parquet и отправляет соответствующие таблицы в базу данных
