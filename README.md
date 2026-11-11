# ДЗ5. Мониторинг антифрода

Kafka → CatBoost на CPU → Kafka → PostgreSQL. Prometheus собирает метрики раз в секунду.

## Запуск

Нужны Docker Compose v2.20+ и 4 ГБ памяти для Docker. Для проверок нужен Python 3.9+.

```bash
git clone https://github.com/SI-py/mts_dz5.git
cd mts_dz5
docker compose up -d --build
```

Первый запуск скачивает образы и обучает модель. Через минуту после старта потока графики заполняются.
`data/demo.csv`: 6000 строк [учебного train.csv](https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394), 5400 обычных и 600 фродовых; доля фрода повышена для демонстрации. Producer непрерывно выбирает строки, обновляет ID и время, исключает target.

| Сервис | Адрес |
|---|---|
| Grafana | [localhost:3000](http://localhost:3000), `admin` / `admin` |
| Prometheus | [localhost:9090](http://localhost:9090) |
| Метрики скоринга | [localhost:8000/metrics](http://localhost:8000/metrics) |
| Метрики записи | [localhost:8001/metrics](http://localhost:8001/metrics) |

Источник данных и дашборды подключаются автоматически. PostgreSQL читается отдельным пользователем с правом SELECT.

## Дашборды

| Дашборд | Панели |
|---|---|
| [Скоринг](http://localhost:3000/d/scoring) | TPS, p50/p95 обработки, доля фрода, скоры во времени, задержка событий, число скоров и доля фрода по версии модели из PostgreSQL |
| [Запись результатов](http://localhost:3000/d/writer) | Скорость и p95 записи, число фрода, скоры, дубли, последние записи |
| [Система](http://localhost:3000/d/system) | CPU, память, сеть, заполнение и I/O диска, доступность сервисов |
| [Бизнес](http://localhost:3000/d/business) | Плотность скоров и TPS с фильтрами `us_state` + `merch`, фрод по категориям, оборот и сумма под риском |

Плотность: `count / (N × 0.05)`, площадь равна 1. TPS: число обработанных событий за 10 секунд / 10; крайний интервал ещё заполняется.
Barplot по `cat_id` считает средний `fraud_flag` в последних 1000 записях всей таблицы, без фильтров периода и сегмента.
Сумма под риском: сумма транзакций с флагом модели, не подтверждённый ущерб. На Docker Desktop системные метрики относятся к Linux VM.

## Метрики

| Имя | Значение |
|---|---|
| `scorer_transactions_total`, `scorer_fraud_total` | Обработанные транзакции и фрод |
| `scorer_processing_seconds`, `scorer_score` | Гистограммы времени обработки и скоров |
| `scorer_fraud_ratio` | Доля фрода в последних 1000 обработанных событиях |
| `scorer_event_lag_seconds` | Возраст события к началу скоринга |
| `scorer_last_transaction_timestamp_seconds` | Время последней обработки, используется алертом |
| `scorer_model_reloads_total{version}`, `scorer_model_loaded_timestamp_seconds{version}` | Число горячих загрузок и момент смены модели |
| `scorer_model_reload_errors_total` | Ошибки загрузки модели; рабочая версия сохраняется |
| `writer_scores_total`, `writer_fraud_total`, `writer_duplicates_total` | Записанные скоры, фрод и повторы ID |
| `writer_insert_seconds`, `writer_score` | Гистограммы времени записи и записанных скоров |
| `node_cpu_seconds_total`, `node_memory_*`, `node_network_*`, `node_filesystem_*`, `node_disk_*` | Системные метрики Node Exporter |

Новая метрика относительно семинара: `scorer_event_lag_seconds`. Панель «Задержка события p95» показывает, что очередь отстаёт от потока, даже если само предсказание быстрое. Доля фрода на графике считается по счётчикам за минуту.

## Проверка и переобучение

```bash
docker compose run --rm trainer python -m unittest discover -s tests -v
python3 scripts/check.py --alerts
```

Проверка ждёт роста счётчиков, выполняет запросы всех панелей, проверяет фильтры, повтор сообщения, смену модели и оба алерта. Итог: `results/check.json`.
В Prometheus запрос `rate(scorer_transactions_total[30s])` показывает поток, а `up` показывает три доступных сборщика.

Проверено 11.11.2026: 4 теста, 2 сквозных прогона, 25 запросов панелей, оба алерта и рестарт. [Результат](results/check.json), [сохранность данных](results/restart.json), [скриншоты](results).

Отдельно переобучить и опубликовать новую версию:

```bash
docker compose run --rm trainer python -m src.train
```

Scorer проверяет указатель модели раз в секунду. На «Скоринге» появляется оранжевая отметка с версией длительностью 30 секунд; она остаётся в истории графика. Таблица версий пополняется без рестарта контейнера. Обучение повторно использует demo.csv.

## Алерты

[Alerting → Alert rules](http://localhost:3000/alerting/list), группа `fraud`. Проверка раз в 10 секунд, условие должно держаться 20 секунд. Доставка сообщений не настроена.

Доля фрода за минуту > 30%. Переключить поток на размеченные фродовые примеры и подождать до 90 секунд:

```bash
docker compose exec producer sh -c 'echo fraud > /control/mode'
```

Нет обработанных событий > 20 секунд. Остановить поступление и подождать до 60 секунд:

```bash
docker compose exec producer sh -c 'echo paused > /control/mode'
```

Вернуть обычный поток; алерты погаснут после обновления минутного окна:

```bash
docker compose exec producer sh -c 'echo normal > /control/mode'
```

## Остановка

```bash
docker compose down
```

`docker compose down -v` удалит данные, модели и историю метрик. После обычного рестарта они сохраняются; повторы Kafka отсекаются по `transaction_id`.
