# Проект Рассылка уведомлений
**Описание**: Приложение для обработки сообщений из Kafka с двумя режимами работы.

## 📌 Функционал
- Режим **обычной обработки** (по расписанию).
- Режим **бесконечного цикла** (`while=true`).
- Graceful Shutdown (корректная обработка `Ctrl+C`).

## 🚀 Запуск
### Обычный режим:
```bash
java -jar app.jar

### Режим бесконечного цикла:
```bash
java -jar app.jar while=true

## Настройка
- в корневом каталоге (где находится исполняемый файл jar) создать каталог 'config'
- в каталоге 'config' скопировать файл настроек 'setting.txt'

## по умолчанию
### настройка KAFKA
TOPIC=topicDVMessage - название Топика KAFKA
BROKER=172.18.8.60:9092,172.18.2.198:9092 - сервера брокер сообщений KAFKA
GROUP_ID=consumer-java-sender - группа сообщений KAFKA

### настройка размерность сообщений
fetch.max.bytes=2147483647
max.partition.fetch.bytes=2147483647
fetch.max.wait.ms=500

### настройка почтового сервера рассылки
SMTP_SERVER=email.itsnn.ru
EMAIL=DVWorkflow@gaz.ru
PASSWORD=

### настройка каталога временного каталога
FILE_PATH=\\%User%\\tmp\\attach\\

### настройка обновременных потоков обработки сообщений
NUM_THREADS=15

### настройка количество попыток обработки
NUM_ATTEMPT=5

### настройка количества сообщений в потоке обработики
LIMIT_SELECT=100

### настройка группы Consumer серверов
SERVER=doc-test_dev

### настройка типов обрабатываемого сообщений
TYPE_MES=1912

### настройка БД Consumer соощений
DB_PATH=\\\\doc-test\\DataBase\\sql_kafka.s3db
