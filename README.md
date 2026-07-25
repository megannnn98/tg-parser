# Comment Cleaner

Конвейер предварительной обработки русскоязычных Telegram-комментариев. Очищает, нормализует и структурирует данные для перевода и NLP-анализа, не изменяя исходный смысл.

## Ограничения (v1.0)

- Без перевода и перефразирования
- Без вызовов LLM и облачных API
- Без определения политической позиции
- Без загрузки содержимого URL
- Без интеграции с Telegram API для загрузки сообщений
- Без исправления орфографии
- Без автоматического удаления спорных сообщений

## Установка

```bash
git clone <repository>
cd telegram-comments

python -m venv .venv
source .venv/bin/activate

pip install -e ".[dev]"
```

## Быстрый старт

```bash
# Создать конфигурацию
cp config.example.yaml my_config.yaml
# Отредактировать input path в my_config.yaml

# Проверить конфигурацию
python -m comment_cleaner validate --config my_config.yaml

# Очистить комментарии
python -m comment_cleaner clean --config my_config.yaml

# Посмотреть статистику
python -m comment_cleaner stats --config my_config.yaml

# Обнаружить дубликаты в уже очищенных данных
python -m comment_cleaner deduplicate --input output/cleaned.jsonl

# Сгруппировать комментарии по пользователям
python -m comment_cleaner group-users --input output/cleaned.jsonl --output user_batches.jsonl

# С псевдонимизацией
export USER_ID_HASH_SALT="your-secret-salt"
python -m comment_cleaner clean --config my_config.yaml --pseudonymize
```

## Контрольная точка и возобновление

Обработку можно возобновить после прерывания. Файлы контрольных точек сохраняются автоматически рядом с выходными файлами.

```yaml
output:
  path: output/cleaned.jsonl
  resume: true           # Включить checkpoint/resume
```

При перезапуске с `resume: true` конвейер:
1. Читает файл контрольной точки (`.checkpoint_cleaned.json` рядом с выходным файлом)
2. Пропускает все сообщения до последнего обработанного `message_id`
3. Дописывает новые данные в существующий выходной файл (без дубликатов)
4. Сохраняет обновлённую контрольную точку после каждого пакета

Чтобы начать заново — удалите файл контрольной точки или установите `resume: false`.

## Цепочка контекста ответов

Конвейер поддерживает построение цепочек ответов глубиной до 3 уровней:

```yaml
context:
  load_reply_context: true
  max_reply_depth: 3     # 0 = отключено, 1-3 уровня
```

Для сообщения, которое отвечает на ответ:
```
Сообщение C (ответ на B) → Сообщение B (ответ на A) → Сообщение A
```

Выходные данные содержат полную цепочку:

```json
{
  "reply_context": {
    "message_id": "B",
    "text": "Текст сообщения B",
    "context_depth": 2,
    "chain": [
      {"message_id": "B", "text": "..."},
      {"message_id": "A", "text": "..."}
    ]
  }
}
```

Защита от циклов предотвращает бесконечные петли при циклических цепочках ответов.

## Режимы обнаружения дубликатов

Поддерживаются три режима:

```yaml
duplicates:
  mode: mark            # keep | mark | collapse
  fuzzy_enabled: true   # Включить нечёткое сравнение (RapidFuzz)
  fuzzy_threshold: 95   # Порог схожести 0-100
```

- **keep**: Без обнаружения дубликатов
- **mark**: Помечать дубликаты без удаления (по умолчанию)
- **collapse**: Пропускать дубликаты в выходных данных

Обнаруживаемые типы дубликатов:
- **exact**: Побайтово идентичный текст
- **normalized**: Совпадает после приведения к нижнему регистру, нормализации пробелов, замены URL/упоминаний
- **fuzzy**: Схожесть выше порога (требуется `fuzzy_enabled: true`)

## Входная схема

### JSONL (по умолчанию)

```json
{"message_id": "154382", "user_id": "928373", "chat_id": "-100123456789", "timestamp": "2026-07-25T10:30:00+05:00", "text": "@ivan ну да конечно 😂 https://example.com", "reply_to_message_id": null, "forwarded_from": null, "message_type": "text"}
```

Рекомендуемый формат: один JSON-объект на строку, кодировка UTF-8, без висячих запятых.

### База данных (SQLite)

```yaml
input:
  type: sqlite
  path: data/app.db
  column_mapping:
    message_id: message_id
    user_id: user
    text: text
    timestamp: date
    reply_to_message_id: reply_to
```

## Выходная схема

```json
{
  "message_id": "154382",
  "user_id": "928373",
  "original_text": "@ivan ну да конечно 😂 https://example.com",
  "cleaned_text": "[MENTION] ну да конечно 😂 [URL]",

  "reply_context": {
    "message_id": "100",
    "text": "Текст родительского сообщения",
    "context_depth": 1,
    "chain": [{"message_id": "100", "text": "Текст родительского сообщения", "user_id": "999"}]
  },

  "quoted_text": null,
  "author_text": "[MENTION] ну да конечно 😂 [URL]",

  "features": {
    "contains_url": true,
    "contains_mention": true,
    "contains_emoji": true,
    "contains_quote": false,
    "contains_reply_context": false,
    "contains_political_terms": false,
    "possible_sarcasm": true,
    "low_information": true,
    "is_duplicate": false,
    "is_bot_message": false,
    "is_system_message": false
  },

  "transformations": [
    {"type": "replace_mention", "original": "@ivan", "replacement": "[MENTION]"},
    {"type": "replace_url", "original": "https://example.com", "replacement": "[URL]"}
  ],

  "urls": [{"original": "https://example.com", "domain": "example.org"}],
  "mentions": ["ivan"],
  "hashtags": [],
  "emoji": ["😂"],
  "emoji_count": 1,
  "detected_terms": [],
  "sarcasm_signals": [{"type": "emoji", "value": "laugh_emoji_with_text"}],
  "information_score": 0.85,
  "processing_version": "1.0.0"
}
```

## Этапы обработки

| Этап | Описание |
|---|---|
| Нормализация Unicode | NFC-нормализация, удаление zero-width символов, нормализация пробелов/переносов, ограничение повторов |
| Обработка URL | Обнаружение URL, извлечение домена, замена на маркер `[URL]` |
| Обработка упоминаний | Обнаружение `@username`, замена на маркер `[MENTION]` |
| Обнаружение хештегов/эмодзи | Извлечение хештегов, обнаружение и подсчёт эмодзи |
| Разбор цитат | Blockquote (`> `), авторский префикс (`писал:`), кавычки (`«»`) |
| Контекст ответов | Поиск родительского сообщения с настраиваемой глубиной цепочки (1-3) |
| Фильтр информативности | Оценка информативности на основе правил |
| Обнаружение сленга | Сопоставление со словарём политического сленга |
| Обнаружение сарказма | Эвристические сигналы: фразы, эмодзи, `/s`, скобки |
| Обнаружение ботов | Системные сообщения и команды ботов |
| Обнаружение дубликатов | Точные, нормализованные и нечёткие дубликаты |

## Все команды CLI

```bash
# Проверить конфигурацию
python -m comment_cleaner validate --config config.yaml

# Очистить комментарии (основной конвейер)
python -m comment_cleaner clean --config config.yaml
python -m comment_cleaner clean --config config.yaml --pseudonymize
python -m comment_cleaner clean --config config.yaml --no-progress

# Дедуплицировать существующий очищенный файл
python -m comment_cleaner deduplicate --input cleaned.jsonl --output deduped.jsonl
python -m comment_cleaner deduplicate --input cleaned.jsonl --mode collapse --threshold 90

# Сгруппировать комментарии по пользователям
python -m comment_cleaner group-users --input cleaned.jsonl --output batches.jsonl
python -m comment_cleaner group-users --input cleaned.jsonl --max-messages 100
python -m comment_cleaner group-users --input cleaned.jsonl --exclude-duplicates --exclude-bots

# Посмотреть статистику очищенных данных
python -m comment_cleaner stats --input output/cleaned.jsonl
```

## YAML-конфигурация

```yaml
input:
  type: jsonl                    # jsonl | sqlite
  path: data/comments.jsonl

output:
  path: output/cleaned.jsonl
  error_path: output/errors.jsonl
  batch_size: 1000
  resume: false                  # Включить checkpoint/resume

normalization:
  unicode_form: NFC
  max_repeated_letters: 3
  max_repeated_punctuation: 3
  max_repeated_brackets: 3
  preserve_emoji: true
  preserve_hashtags: true

urls:
  replace_with_marker: true
  marker: "[URL]"
  save_domain: true

mentions:
  replace_with_marker: true
  marker: "[MENTION]"

duplicates:
  mode: mark                     # keep | mark | collapse
  fuzzy_enabled: false
  fuzzy_threshold: 95

context:
  load_reply_context: true
  max_reply_depth: 1             # 0-3

filtering:
  remove_system_messages: false
  remove_bot_messages: false
  remove_low_information: false

privacy:
  pseudonymize_user_ids: false
  salt_env_variable: "USER_ID_HASH_SALT"

sarcasm_detection:
  enabled: true

processing_version: "1.0.0"
```

## Приватность данных

- Исходный текст **всегда сохраняется** без изменений
- Псевдонимизация использует HMAC-SHA256 с солью из переменной окружения
- Соль НЕ должна храниться в репозитории
- Номера телефонов и email маскируются: `[PHONE]`, `[EMAIL]`
- Полный текст комментариев никогда не пишется в лог на уровне INFO
- Псевдонимизация детерминирована: один user_id → один и тот же хеш во всех сообщениях

## Разработка

```bash
pip install -e ".[dev]"

# Запуск тестов
pytest tests/comment_cleaner/ -v

# Запуск тестов с покрытием
pytest tests/comment_cleaner/ --cov=comment_cleaner --cov-report=term

# Линтер
ruff check src/ tests/

# Проверка форматирования
ruff format --check src/ tests/

# Проверка типов
mypy src/comment_cleaner/
```

## Лицензия

MIT
