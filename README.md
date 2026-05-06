# BigDataFlink
Анализ больших данных - лабораторная работа №3 - Streaming processing с помощью Flink

## Содержание
1. Структура проекта
2. Схема dwh
3. Принцип работы
4. Запуск проекта

## Структура проекта

```/data``` - исходные CSV-файлы для анализа    

```/flink_lib``` - JDBC-драйверы для работы Flink, Kafka, PostgreSQL 

```/flink_processor```   
- ```processor.sql``` - файл для определения таблиц-источников (Kafka) и приёмников (PostgreSQL), выполнения потоковой трансформации данных из JSON-сообщений Kafka в нормализованную схему "звезда"

```/init_sql_scripts``` - SQL-скрипты для инициализации PostgreSQL (создание таблиц схемы "звезда")

```/kafka_producer```
- ```producer.py``` - файл для чтения строк из CSV-файлов, преобразования их в JSON и отправки в Kafka-топик для последующей обработки Flink
- ```requirements.txt```- список необходимых зависимостей (установка автоматически при запуске docker-compose через pip)

```docker-compose.yml```

## Схема dwh
1. dim_customer - измерение "Покупатель"
2. dim_customer_pet - измерение "Питомец покупателя"
3. dim_seller - измерение "Продавец"
4. dim_supplier - измерение "Поставщик"
5. dim_store - измерение "Магазин"
6. dim_product - измерение "Товар"
7. fact_sales - таблица фактов "Продажи"

## Принцип работы
Система построена на потоковой обработке данных с использованием Apache Kafka, Apache Flink и PostgreSQL.  
Сначала Python-скрипт ```producer.py``` читает строки из csv-файлов, преобразует каждую строку в формат JSON и отправляет сообщения в Kafka-топик ```mock_data_input```.   
Далее запущенная в Flink SQL-джоба (```processor.sql```) непрерывно потребляет сообщения из Kafka, выполняет трансформацию данных: десериализует JSON, приводит типы, выделяет уникальные записи для таблиц измерений и вставляет их в соответствующие таблицы в PostgreSQL, а также формирует таблицу фактов, связывающую все измерения.  
В результате в базе данных PostgreSQL автоматически наполняется схема "звезда", готовая для аналитических запросов.

## Запуск проекта
Для удобства сразу откройте 3 терминала в корневой папке проекта.

### 1. Запуск инфраструктуры
В терминале 1 выполните команду:  
```bash
docker compose up -d postgres zookeeper kafka jobmanager taskmanager
```

Эта команда запустит основную инфраструктуру, но producer запустим вручную позже, поскольку перед этим нужно поднять основные сервисы.

Проверьте контейнеры в терминале 1 с помощью команды:  
```bash
docker compose ps
```

Дождитесь статусов ```running``` или ```healthy```. Это может занять пару минут.
Если возникает ошибка о том, что какой-то порт уже занят, измените порт в docker-compose.yml или временно остановите другие контейнеры, которые с ним взаимодействуют.

### 2. Создание Kafka topic
В терминале 1 выполните команду:  
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --create --if-not-exists --topic mock_data_input --partitions 1 --replication-factor 1
```

Проверьте, что топик создался, с помощью:
```bash
docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list
```
Команда должна вывести:  
```mock_data_input```

### 3. Запуск Flink-job
В терминале 2 выполните команду:
```bash
docker exec -it jobmanager /opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/processor.sql
```

Вы можете проверить работоспособность Flink, перейдя в браузере по ссылке:  
```http://localhost:8081```  
В Flink Web UI среди Jobs / Running Jobs должна появиться наша запущенная джоба.

### 4. Запуск producer
В терминале 3 выполните команду:
```bash
docker compose run --rm kafka-producer
```

Эта команда запускает основной пайплайн. В конце вывода вы должны увидеть что-то подобное:
```
Detected 10 csv-files.
Processing: MOCK_DATA_0.csv, index = 0
Processing: MOCK_DATA_1.csv, index = 1
...
Sent 10000 messages.
```

После появления последней надписи можно перейти к проверке.

### 5. Проверка PostgreSQL
В терминале 1 (или новом) зайдите в консоль PostgreSQL с помощью команды:
```bash
docker exec -it postgres psql -U postgres -d bigdata_flink
```

Введите:
```sql
\dt dwh.*
```
Эта команда выведет все созданные таблицы.

Проверьте количество строк:
```sql
select count(*) from dwh.fact_sales;
select count(*) from dwh.dim_customer;
select count(*) from dwh.dim_customer_pet;
select count(*) from dwh.dim_seller;
select count(*) from dwh.dim_supplier;
select count(*) from dwh.dim_store;
select count(*) from dwh.dim_product;
```

Таблица ```dwh.fact_sales``` должна содержать 10000 строк.

Также можно выполнить проверочные аналитические запросы, например посмотреть первые строки в различных таблицах:
```sql
select *
from dwh.fact_sales
limit 10;
```

```sql
select *
from dwh.dim_customer
limit 10;
```

```sql
select *
from dwh.dim_product
limit 10;
```

Для выхода из консоли PostgreSQL введите:
```bash
\q
```

### 6. Завершение работы
В Flink Web UI перейдите в Jobs → Running Jobs → выбрать job → Cancel Job. Так вы остановите Flink-джобу, работающую в running-режиме.

Остановите окружение (в терминале 1) с удалением volumes:
```bash
docker compose down -v
```
