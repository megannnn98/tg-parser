# telegram-comments

Собирает комментарии из discussion-чатов, привязанных к Telegram-каналам, и складывает их в PostgreSQL.

Схема базы, перенос данных из SQLite и резервные копии описаны в
[docs/postgresql-migration.md](docs/postgresql-migration.md), исследование нарезки
комментариев и эмбеддингов — в
[docs/embedding-chunking-research.md](docs/embedding-chunking-research.md).

## Локальные эмбеддинги и сравнение авторов

На странице пользователя раздел «Похожие высказывания авторов» создаёт эмбеддинги
самих комментариев всех пользователей, чьи комментарии собраны режимом
`user-comments`. Нажмите
«Создать эмбеддинги и сравнить». Панель показывает чтение истории, создание векторов
и сравнение авторов, текущую операцию, количество обработанных текстов и кеш.

Раздел не считает векторы заново: он берёт готовые эмбеддинги комментариев из
таблицы `message_embeddings`, те же, по которым работает поиск по смыслу
(`intfloat/multilingual-e5-small`, 384 числа). Ключ OpenRouter для этого раздела
не нужен, комментарии во внешний API не отправляются.

Перед сравнением приложение само достраивает эмбеддинги тех комментариев
собранных профилей, у которых их ещё нет. Для этого при первом запуске веса модели
скачиваются из Hugging Face в `DATA_DIR/e5-cache`; модель работает на CPU или
доступной CUDA. Комментарий длиннее 512 токенов представлен своим началом.
После обновления `requirements.txt` пересоберите образ командой
`docker build -f ci/Dockerfile -t telegram-parser .`, чтобы установить зависимости
локального анализа, включая NumPy, PyTorch и Transformers.

Модель, размерность, путь и число векторов показаны в раскрывающемся блоке панели.

Перед сравнением из каждого вектора вычитается общая часть — среднее средних
векторов всех авторов. Без этого любые два автора получают сходство около 0,99:
векторы E5 у любых текстов близки, а усреднение сотен комментариев оставляет
почти одно общее.

Рейтинг строится по косинусному сходству нормализованных средних векторов уникальных
комментариев каждого автора. Повторение одного текста не увеличивает его вес.
Показываются до десяти ближайших авторов; автор с неположительным сходством
похожим не считается и в список не попадает. Для каждого — до пяти пар самых
близких по смыслу комментариев двух авторов: каждый комментарий одного сравнивается
с каждым комментарием другого по исходным векторам, как в поиске по смыслу.
Комментарии короче 80 символов и текст, который написали оба автора, в пары не
попадают. Сходство авторов лежит между −1 и 1;
это сходство текстов, не оценка политического согласия. Автоматического выделения
только политических комментариев или последней позиции в этом режиме нет.

Повторный запуск использует кеш векторов и кодирует только новые тексты. После
остановки можно продолжить; последний готовый рейтинг остаётся доступным при
ошибке обновления. Старые результаты DeepSeek не выдаются за локальное сходство.
Предыдущий анализ политического согласия описан в
`docs/political-position-comparison.md`; он больше не запускается этой кнопкой.
Отдельный анализ политических координат продолжает использовать свои настройки
OpenRouter.

## Подготовка

1. Положите настройки в `.env` (для запуска с хоста) и в `.env.docker` (для контейнера):

```
API_ID=1234567
API_HASH=0123456789abcdef0123456789abcdef
LIMIT=1000
LOG_LEVEL=INFO
DATABASE_URL=postgresql+asyncpg://telegram:telegram@127.0.0.1:5435/telegram_comments
APP_TIMEZONE=Asia/Almaty
```

   В контейнере `DATABASE_URL` подставляет `docker-compose.yml` (хост `postgres`), поэтому в
   `.env.docker` он не обязателен. Пароль `telegram` годится только для локальной базы,
   порт которой открыт на `127.0.0.1`; другой задаётся переменной `POSTGRES_PASSWORD`.

2. Соберите образ и поднимите PostgreSQL с pgvector:

```
docker compose build app
docker compose up -d postgres
```

3. Создайте схему базы:

```
./scripts/run.sh migrate          # то же, что alembic upgrade head
```

   Эту команду нужно повторять после обновления кода: новые миграции лежат в
   `alembic/versions`.

4. Один раз войдите в Telegram интерактивно — Pyrogram спрашивает номер телефона и код, а
   `scripts/run.sh` запускается без TTY, поэтому первый вход нужно сделать вручную:

```
docker run --rm -it --user "$(id -u):$(id -g)" --env-file .env.docker -v "$PWD:/app" --entrypoint python telegram-parser -m scripts.login
```

Сессия сохраняется в `my_session.session`; после этого `./scripts/run.sh <режим>` работает
без TTY.

### Если код подтверждения не приходит

Первый код Telegram отправляет не по SMS, а сообщением в сервисный чат «Telegram» в уже
залогиненных приложениях (проверьте и «Архив»). Встроенный вход Pyrogram повторно код не
запрашивает, поэтому если сообщения нет, войдите через `scripts/login.py`:

```
docker run --rm -it --user "$(id -u):$(id -g)" --env-file .env.docker -v "$PWD:/app" --entrypoint python telegram-parser -m scripts.login
```

1. Остановите зависший вход в другом окне (Ctrl-C), если он ещё запущен.
2. Введите номер телефона. Скрипт напечатает, куда ушёл код и чем он будет отправлен при
   повторе:

```
Code sent via APP. Resend would use SMS, allowed after 60 s.
```

3. Если кода нет, подождите указанное число секунд и нажмите Enter на пустой строке — код
   уйдёт следующим способом (обычно по SMS). Повторять можно несколько раз.
4. Введите код. Если включена двухэтапная аутентификация, скрипт спросит пароль.
5. После строки `Logged in, session saved.` сессия записана в `my_session.session`.

Ошибки Telegram (неверный или истёкший код, слишком ранний повтор) скрипт печатает и
спрашивает код снова, перезапускать его не нужно. Если Telegram ответил `FLOOD_WAIT`,
скрипт завершится с числом секунд ожидания — частые запросы кода продлевают этот срок.

## Сбор комментариев одного пользователя по всем каналам

```
./scripts/run.sh user-comments @Mega_Palez
```

Режим находит пользователя, записывает его в таблицу `users`, проходит каналы из
`channels.json` и сохраняет его комментарии в общую таблицу `messages`. Отдельный файл на
пользователя больше не создаётся.

Принимает и `@username`, и числовой `tg_id`. Пользователь, которого уже собирали,
повторно в Telegram не ищется: запросы поиска по нику Telegram ограничивает на часы.

Фильтрация выполняется на стороне Telegram (`search_messages(from_user=...)`), поэтому
забирается вся история пользователя в каждом discussion-чате — `LIMIT` в этом режиме не
применяется.

Вывод:

```
Resolved user:
tg_id=123456789
username=Mega_Palez

channels scanned: 143
channels failed: 0
messages fetched: 1766
new messages: 131
duplicates: 1635
```

Повторный запуск дозабирает только новое: комментарий определяется парой «канал и номер
сообщения», и уже сохранённый второй раз не записывается. Текст сохранённого комментария
не перезаписывается; чтобы взять его из Telegram заново, добавьте `--refresh-text`.

Посмотреть, что записалось:

```
docker compose exec postgres psql -U telegram -d telegram_comments -c "
  SELECT c.username, count(*)
  FROM messages m
  JOIN users u ON u.id = m.user_id
  JOIN channels c ON c.id = m.channel_id
  WHERE u.tg_id = 123456789
  GROUP BY c.username ORDER BY 2 DESC;"
```

Каналы без привязанного обсуждения логируются и пропускаются
(`Channel <name> has no linked discussion`). Канал, упавший по любой другой причине
(неверный username, нет доступа, длинный `FloodWait`), логируется с трейсбеком и
пропускается, а проход продолжается; уже сохранённое остаётся. Мёртвая сессия (отозвана,
auth key не зарегистрирован, аккаунт деактивирован) наоборот прерывает проход — в логе
указано, сколько строк уже успело сохраниться.

## Сбор комментариев каналов

```
./scripts/run.sh collect
```

Читает последние `LIMIT` сообщений обсуждения каждого канала и сохраняет их вместе с
авторами. До восьми каналов читаются одновременно, каждый пишет пакетами по 500
сообщений. Авторы, встреченные так, попадают в `users`, но профилями в веб-интерфейсе
становятся только те, по кому запускали `user-comments`.

## Веб-профиль пользователя

Веб-интерфейс читает PostgreSQL. Соберите фронтенд (один раз и после каждого изменения в
`frontend/`; нужен Node.js 22.13+, на Arch: `sudo pacman -S nodejs npm`) и запустите:

```
(cd frontend && npm ci && npm run build)
./scripts/run.sh web
```

FastAPI отдаёт сборку из `frontend/dist` (другой путь можно задать в `FRONTEND_DIST`), а
JSON API живёт под `/api/v1` (схема — `frontend/openapi/openapi.json`). Без сборки любая
страница отвечает `503` с подсказкой, как её получить. Вместо локальной сборки можно
скачать артефакт `frontend-dist` последнего прогона GitHub Actions и распаковать его в
`frontend/dist`.

Откройте в браузере `http://localhost:8000`. На главной странице — пользователи, чьи
комментарии собраны. Страница пользователя (`/users/<tg_id>`) показывает ник, `tg_id`,
количество сообщений и каналов, распределение по каналам и активность по часам, дням и
дням недели. Пользователь адресуется по `tg_id`: ник может смениться.

Часы и дни активности считаются в поясе `APP_TIMEZONE`.

Сайт закрыт паролем из переменной `WEB_PASSWORD`: без неё веб-приложение не запускается.
Задайте её в `.env.docker` (для запуска без Docker — в `.env`). После входа браузер хранит
подписанную cookie 30 дней; смена пароля завершает все сессии. Пять неверных паролей с
одного адреса за пять минут закрывают вход с этого адреса до конца этих пяти минут.
Без входа доступны только страница входа и файлы сборки, любой запрос к `/api/v1` отвечает
`401`.

Если порт `8000` занят: `WEB_PORT=8010 ./scripts/run.sh web`.

Без Docker:

```
.venv/bin/python -m uvicorn web.app:app --host 127.0.0.1 --port 8000
```

Быстрая проверка из терминала:

```
curl -sS http://127.0.0.1:8000/api/v1/profiles
curl -sS http://127.0.0.1:8000/api/v1/users/123456789/comments.txt
```

Если список пуст, проверьте, что миграции применены и что пользователь собран:

```
docker compose exec postgres psql -U telegram -d telegram_comments -c "
  SELECT tg_id, username, profile_collected_at FROM users
  WHERE profile_collected_at IS NOT NULL;"
```

## Фронтенд

`frontend/` — React 19 + TypeScript + Vite, Tailwind CSS 4 и компоненты shadcn/ui,
данные через TanStack Query, роутинг react-router; тот же стек, что в соседнем проекте
ebnv. Клиент API (`src/api/generated`) генерируется из OpenAPI-схемы бэкенда:

```
cd frontend
npm run dev            # dev-сервер на :5173, /api/v1 проксируется на :8000
npm run typecheck
npm test
PYTHON=../.venv/bin/python npm run generate:api   # после изменения API
```

`frontend/openapi/openapi.json` и `src/api/generated` закоммичены; `tests/test_web_app.py`
падает, если схема устарела.

## Android-клиент

В каталоге `android/` лежит отдельный Gradle-модуль — минималистичный APK, который
открывает этот web UI во встроенном `WebView`. Приложение не дублирует логику
парсера: список каналов, скачивание комментариев, профиль пользователя — всё
остаётся на стороне FastAPI. Нативная часть только одна — экран настроек, куда
вписывается URL бэкенда. Подробности сборки и ограничений — в
[`android/README.md`](android/README.md).

### Что нужно для запуска

1. **Где-то уже работает FastAPI-сервер `telegram-comments`** (см. раздел выше).
   Если телефон подключается к компьютеру по Wi-Fi, поднимайте сервер на `0.0.0.0`,
   иначе он слышит только `127.0.0.1`:
   ```
   WEB_PORT=8000 .venv/bin/python -m uvicorn web.app:app --host 0.0.0.0 --port 8000
   ```
   IP компьютера на локальной сети смотрят так:
   ```
   ip -4 addr show | grep -oP '(?<=inet\s)\d+(\.\d+){3}'
   ```
   Допустим, получилось `192.168.1.5` — тогда URL для APK:
   `http://192.168.1.5:8000`.

2. **APK собран и установлен на телефон** (Android 8.0/API 26 и выше). Сборка —
   через Android Studio (`File → Open` каталога `android/`, далее `Build → Build
   APK(s)`) или из CLI:
   ```
   echo "sdk.dir=$HOME/Android/Sdk" > android/local.properties
   (cd android && ./gradlew assembleDebug)
   ```
   Готовый файл — `android/app/build/outputs/apk/debug/app-debug.apk`.
   Нужны JDK 17 (JBR из Android Studio подходит, JDK 26 — нет) и Android SDK с
   `platforms;android-35` и `build-tools;35.0.x`. При установке на телефон
   разрешите «установку из неизвестных источников».

### Установка APK через adb (альтернатива sideload)

Если на компьютере есть Android SDK Platform Tools, быстрее ставить APK через
`adb`, а не через файл-менеджер с разрешением «установка из неизвестных
источников».

1. Включите на телефоне отладку по USB: *Настройки → О телефоне* → тапнуть
   «Номер сборки» 7 раз, затем *Настройки → Система → Для разработчиков →
   Отладка по USB*.
2. Подключите телефон кабелем, разрешите отладку в появившемся диалоге.
3. На компьютере:
   ```
   adb install android/app/build/outputs/apk/debug/app-debug.apk
   ```
   После пересборки — `adb install -r ...` (заменяет установленную копию,
   сохраняя данные приложения, включая сохранённый URL).
4. Логи APK и WebView — через `adb logcat` (см. раздел «Логи и отладка»).

`adb` входит в `android-sdk/platform-tools/`; отдельная загрузка — на
[developer.android.com/studio/releases/platform-tools](https://developer.android.com/studio/releases/platform-tools).

### Первый запуск

1. Откройте приложение «Telegram Comments». При первом запуске появится экран
   «Бэкенд не настроен».
2. Тапните «Открыть настройки», введите URL бэкенда
   (`http://192.168.1.5:8000` или `https://comments.example.ru`), нажмите
   «Сохранить и открыть».
3. Откроется главная страница web UI. Дальше всё как в браузере: список
   пользователей, кнопка «Скачать комментарии по юзернейму», редактор списка
   каналов.

Back-кнопка ходит по истории WebView (а не закрывает приложение сразу). Меню
(три точки) содержит «Обновить» и «Настройки». Если бэкенд недоступен —
показывается экран ошибки с кнопкой «Обновить».

### HTTPS и cleartext

Приложение разрешает cleartext HTTP, чтобы работал адрес вида
`http://192.168.1.5:8000` (см.
[`android/app/src/main/res/xml/network_security_config.xml`](android/app/src/main/res/xml/network_security_config.xml)).
Это безопасно внутри домашней сети; для публичного деплоя поднимите HTTPS на
FastAPI или за reverse-proxy и впишите в настройки `https://...`. Самоподписанные
сертификаты не поддерживаются — только системные.

### Бэкенд на самом телефоне через Termux

> После перехода на PostgreSQL серверу нужна база: задайте `DATABASE_URL` на доступный
> с телефона PostgreSQL с расширением pgvector и выполните `alembic upgrade head`.
> Шаги ниже написаны до перехода и установку базы не описывают; на телефоне этот
> вариант после перехода не проверялся.

Если отдельного компьютера нет и FastAPI-сервер должен работать на самом
телефоне, используйте **Termux** — нативный Linux-терминал под Android. Существующий
Python-код (парсер, FastAPI, pyrogram) запускается в нём без изменений; WebView-APK
на том же телефоне подключается к `http://127.0.0.1:8000`.

1. **Поставьте Termux** — только версию с [F-Droid](https://f-droid.org/packages/com.termux/);
   вариант в Google Play устарел и не обновляется.
2. В Termux поставьте Python и git:
   ```
   pkg update && pkg install -y python git
   ```
3. Склонируйте репозиторий и поставьте зависимости:
   ```
   git clone https://github.com/megannnn98/tg-parser.git telegram-comments
   cd telegram-comments
   pip install -r requirements.txt
   ```
   Если `pip` упадёт на сборке `tgcrypto` (C-extension), либо поставьте
   `pkg install -y build-essential clang` и повторите, либо удалите
   `tgcrypto` из `requirements.txt` — pyrogram работает и без него через
   `pyaes`, медленнее в несколько раз, но для личного использования
   незаметно.
4. **Один раз войдите в Telegram** интерактивно:
   ```
   cp .env.docker .env    # при желании поправьте API_ID/API_HASH/LIMIT
   python main.py collect
   ```
   Pyrogram спросит номер телефона и код из основного приложения Telegram;
   сессия сохранится в `my_session.session` внутри Termux.
5. **Положите сборку фронтенда.** Node.js в Termux не нужен: скачайте артефакт
   `frontend-dist` последнего прогона GitHub Actions (вкладка Actions → прогон →
   Artifacts) и распакуйте его в `frontend/dist`:
   ```
   mkdir -p frontend/dist && unzip -o ~/storage/downloads/frontend-dist.zip -d frontend/dist
   ```
   (`~/storage` появляется после `termux-setup-storage`.) Повторяйте после `git pull`,
   если менялся `frontend/`.
6. **Запустите web UI**:
   ```
   termux-wake-lock
   python -m uvicorn web.app:app --host 127.0.0.1 --port 8000
   ```
   `termux-wake-lock` удерживает CPU активным, чтобы Android не убил процесс
   в фоне. После остановки uvicorn отпустите блокировку: `termux-wake-unlock`.
7. В WebView-приложении впишите URL `http://127.0.0.1:8000`.

**Ограничения Termux-пути:**

- Android всё равно может убить Termux при сильной нехватке памяти.
  Долгие скачивания (`user-comments` на большом списке каналов,
  `discover-channels`) запускайте с телефоном на зарядке и Termux в
  foreground.
- Автозапуск при загрузке телефона — отдельное приложение
  [Termux:Boot](https://wiki.termux.com/wiki/Termux:Boot) со скриптом в
  `~/.termux/boot/`.
- Батарея: pyrogram держит WebSocket к Telegram открытым; расход заметный,
  для длительных сессий — телефон на зарядку.
- Повторный вход в ту же Telegram-аккаунт с телефона параллелен десктопной
  сессии — Telegram это разрешает, но при определённых условиях может
  потребовать повторного ввода кода.

### Termux:Boot — автозапуск бэкенда при загрузке телефона

Приложение [Termux:Boot](https://wiki.termux.com/wiki/Termux:Boot) от тех же
авторов запускает скрипты из `~/.termux/boot/` сразу после загрузки Android.
Один раз настроив, бэкенд будет подниматься без ручного открывания Termux.

1. Поставьте Termux:Boot с F-Droid (того же источника, что и сам Termux).
2. Откройте его один раз — это зарегистрирует приложение для старта при
   загрузке. UI у него нет.
3. В Termux создайте скрипт запуска:
   ```
   mkdir -p ~/.termux/boot
   cat > ~/.termux/boot/start-backend <<'EOF'
   #!/data/data/com.termux/files/usr/bin/sh
   termux-wake-lock
   cd ~/telegram-comments
   exec python -m uvicorn web.app:app --host 127.0.0.1 --port 8000
   EOF
   chmod +x ~/.termux/boot/start-backend
   ```
4. Перезагрузите телефон. После загрузки Termux:Boot запустит скрипт;
   бэкенд поднимется автоматически.

Замечания:

- Если Android всё равно убивает процесс (особенно на MIUI/EMUI с их
  агрессивной экономией батареи) — добавьте Termux и Termux:Boot в список
  «не оптимизировать» в настройках батареи.
- Путь `~/telegram-comments` предполагает, что репозиторий склонирован в
  home Termux; поправьте под себя.
- Логи автозапуска скрипт не пишет. При проблеме временно замените
  `exec python ...` на `python ... 2>&1 | tee ~/boot.log` и перезагрузитесь.

### Логи и отладка

**WebView-APK.** В debug-сборке включён
`WebView.setWebContentsDebuggingEnabled(BuildConfig.DEBUG)` (см.
[`MainActivity.kt`](android/app/src/main/java/com/telegramcomments/app/MainActivity.kt)),
поэтому страницу внутри APK можно инспектировать с компьютера через Chrome
DevTools:

1. Подключите телефон по USB с включённой отладкой (см. раздел про adb выше).
2. На компьютере откройте `chrome://inspect/#devices`.
3. Под названием устройства появится *Telegram Comments* с кнопкой *inspect*.
   Откроется обычный DevTools: Console, Network, Sources — весь фронтенд
   FastAPI-сайта виден, как если бы это была вкладка Chrome.

Нативные логи APK (включая колбэки `WebViewClient.onReceivedError`):
```
adb logcat -s WebViewGL AndroidRuntime ActivityManager
```
или без фильтра: `adb logcat | grep -iE 'telegramcomments|webview'`.

В release-сборке `BuildConfig.DEBUG = false`, и DevTools по `chrome://inspect`
не подключается — намеренно, чтобы не утекал фронтенд-стейт в
распространяемой сборке.

**Termux / Python.** Логи pyrolog и uvicorn идут в stdout Termux. Чтобы
писать их в файл:
```
python -m uvicorn web.app:app --host 127.0.0.1 --port 8000 2>&1 | tee uvicorn.log
```
Также парсер пишет в `data/telegram-comments-classify.log` и
`data/telegram-comments-translate.log` (см. корневой `.gitignore`).

Проверить, активна ли `termux-wake-lock`, отдельной командой нельзя —
индикатор виден в шторке уведомлений (постоянное уведомление Termux «Wake
lock held»). Снять блокировку: `termux-wake-unlock`.

## Поиск пользователя по отображаемому имени

`user-comments` принимает только `@username` или числовой `tg_id` — отображаемое имя
(«Хрюкало Офф») Telegram API не резолвит. Чтобы получить идентификатор по имени,
есть режим `find-user`:

```
./scripts/run.sh find-user "Хрюкало"
```

По каждому каналу сначала выполняется быстрый серверный поиск среди участников
обсуждения; если там никого не нашлось — **или** если поиск участников недоступен
(он требует членства в discussion-чате, в отличие от чтения истории) — просматриваются
авторы истории чата. Так находятся и те, кто из чата уже вышел. Совпадение ищется по
нику, имени и фамилии, без учёта регистра. Результат:

```
tg_id     | username  | name        | found in | channels
555123456 | @hryukalo | Хрюкало Офф | members  | rud01vb, d_tyazhkun
777000111 | —         | Хрюкало     | history  | lenin_crew

Собрать комментарии:
    ./scripts/run.sh user-comments 555123456
    ./scripts/run.sh user-comments 777000111
```

Ищите по одному слову («Хрюкало»), а не по полному имени: поиск среди участников
выполняет Telegram по своим правилам, и полная строка может не совпасть.

Две границы применимости:

- поиск по истории видит только последние `LIMIT` сообщений чата;
- `get_chat_members` в супергруппах может отдавать неполный список участников.

Поэтому пустой результат не доказывает, что человек не комментировал. И наоборот:
отображаемое имя не уникально — если кандидатов несколько, различайте их по `tg_id`.
Он не меняется никогда, в отличие от ника и имени, поэтому найденный `tg_id` стоит
записать.

## Расширение списка каналов

```
./scripts/run.sh discover-channels
```

Проходит по каналам из `channels.json` и для каждого сканирует **и посты самого
канала, и его обсуждение** (по `LIMIT` сообщений на источник), собирая кандидатов из
двух источников:

- репосты — `forward_from_chat` пересланного сообщения;
- ссылки и упоминания в тексте и подписях к медиа: `t.me/name`, `t.me/name/123`, `@name`.

Инвайт-ссылки (`t.me/+...`, `/joinchat/`), служебные пути (`t.me/c/...`, `t.me/s/...`,
`addstickers` и прочие) и адреса почты кандидатами не считаются.

Кандидаты проверяются в порядке убывания числа упоминаний — на каждого уходит один
запрос, поэтому сначала тратятся на самых популярных. Годным считается **только канал
с привязанным обсуждением**: без него комментариев нет и для `collect`/`user-comments`
он бесполезен. Супергруппы отсекаются по типу, даже если у них есть `linked_chat` (у
чата-обсуждения он указывает на его собственный канал). Обход прекращается, когда
список дорастает до `DISCOVER_TARGET` (по умолчанию 200).

Каждый добавленный канал попадает в лог с названием, числом подписчиков и числом
упоминаний — по этому можно судить, что за канал:

```
2026-08-01 13:20:41,006 | INFO | channel_discovery | [rud01vb] 34 candidate(s) mentioned
2026-08-01 13:21:05,412 | INFO | channel_discovery | 58 unique candidate(s) to check
2026-08-01 13:21:07,880 | INFO | channel_discovery | + @lenin_crew — 'Ленин Крю', 41230 members, 12 mention(s)
2026-08-01 13:21:09,133 | INFO | channel_discovery | + @spichka_media — 'Спичка', 8800 members, 7 mention(s)
2026-08-01 13:21:31,590 | INFO | main | Added 47 channel(s) to channels.json: 11 -> 58
```

Три вещи, о которых стоит знать:

- **Один запуск — один уровень.** За проход обходятся только те каналы, что уже лежат
  в `channels.json`. Но найденное туда же и дописывается, поэтому следующий запуск идёт
  по расширенному списку — до 200 обычно доходят за 2–3 запуска.
- **Тематика не фильтруется.** В улове будут реклама, зеркала и случайные каналы;
  список нужно вычитывать руками, ориентируясь на лог.
- **Риск `FloodWait`.** Резолв десятков имён подряд Telegram может притормозить.
  Pyrogram сам ждёт при задержке до 60 с, дольше — кандидат пропускается с логом.

## Чанки и эмбеддинги

Нарезка и эмбеддинги строятся из уже сохранённых комментариев; Telegram для этого не
нужен, и смена способа нарезки или модели не требует скачивать комментарии заново.

```
# чанки одной стратегии; несколько стратегий и наборов параметров хранятся рядом
python -m scripts.build_chunks --strategy token_budget \
    --params '{"max_tokens": 256, "overlap": 0, "same_channel": true}'

# эмбеддинги сообщений и чанков набора
python -m scripts.embed --messages
python -m scripts.embed --chunk-set 1
```

Рабочая модель — `intfloat/multilingual-e5-small` (384 измерения), она же используется по
умолчанию. Поверх этих эмбеддингов работает поиск по смыслу на странице пользователя и
`GET /api/v1/users/<tg_id>/search?q=...`; как он ранжирует, описано в
[docs/semantic-search.md](docs/semantic-search.md).

Повторный запуск считает только недостающее. `--force` пересобирает всё. Стратегии:
`message`, `fixed_messages`, `token_budget`, `time_window`, `hybrid`, `hybrid_short`;
их параметры описаны в `chunking/strategies.py`. Нужны PyTorch и Transformers.

## Замеры

```
python -m benchmarks.corpus_stats                      # размеры сообщений
python -m benchmarks.chunking_benchmark                # сравнение стратегий нарезки
python -m benchmarks.chunking_benchmark --postgres     # плюс размер и скорость в pgvector
python -m benchmarks.chunking_benchmark --compare-models
python -m benchmarks.chunking_benchmark --sample 300 --only "C tokens 256"   # быстрая проверка
python -m benchmarks.postgres_benchmark                # запись и чтение PostgreSQL
```

Сравнение стратегий читает базу и считает эмбеддинги в `data/benchmark`, в рабочие
таблицы ничего не пишет. Ему нужен файл `benchmarks/eval/mapping.local.json`, которого
нет в репозитории: см. `benchmarks/eval/README.md`.

## Данные

Все данные лежат в одной базе PostgreSQL:

```
users(id, tg_id UNIQUE, username, first_name, last_name, profile_collected_at)
channels(id, username UNIQUE, telegram_chat_id, linked_chat_id)
messages(id, tg_message_id, user_id -> users, channel_id -> channels, text, date,
         UNIQUE(channel_id, tg_message_id))
```

Текст хранится таким, каким пришёл из Telegram, дата — с часовым поясом. Чанки,
эмбеддинги и кеш анализа позиций — производные таблицы, они пересчитываются из
`messages`. Полная схема — в [docs/postgresql-migration.md](docs/postgresql-migration.md).

Данные из старых файлов SQLite переносятся один раз:

```
python -m scripts.import_sqlite --data-dir data
```

### Резервная копия

```
docker compose exec -T postgres pg_dump -U telegram -d telegram_comments -Fc > telegram_comments.dump
docker compose exec -T postgres pg_restore -U telegram -d telegram_comments --clean --if-exists < telegram_comments.dump
```

## Выкладка на сервер

Сервер запускает образ `deploy/Dockerfile`: код и собранный фронтенд лежат внутри него,
из проекта ничего не монтируется. `docker-compose.prod.yml` поднимает PostgreSQL (без порта
наружу) и сайт на `127.0.0.1:8002`; наружу его отдаёт reverse proxy с HTTPS. Пароль входа
передаётся в открытом виде в теле запроса, поэтому без HTTPS сайт выкладывать нельзя.

Сессия Telegram, список каналов и кеши моделей хранятся в томе `app-data`
(`/app/data` в контейнере) и переживают пересборку образа.

Первый раз, на сервере:

```
git clone https://github.com/megannnn98/tg-parser.git /root/telegram-comments
cd /root/telegram-comments
vim .env
```

В `.env`:

```
API_ID=...
API_HASH=...
WEB_PASSWORD=...          # пароль входа на сайт, длинный и случайный: openssl rand -base64 24
POSTGRES_PASSWORD=...     # только буквы и цифры: openssl rand -hex 24
OPENROUTER_API_KEY=...    # для «Определить полит. взгляды»
APP_TIMEZONE=Asia/Almaty
```

Выкладка и каждое следующее обновление, с рабочего компьютера:

```
ssh root@SERVER 'bash -s' < deploy/deploy.sh
```

Скрипт проверяет рабочее дерево, делает `pg_dump` в `backups/`, обновляет код
(`BRANCH=...` выбирает ветку, по умолчанию `main`), собирает образ, применяет миграции,
пересоздаёт сайт и проверяет, что страница входа отвечает `200`, а API без входа — `401`.
Пересоздание сайта обрывает идущий сбор комментариев или анализ.

Вход в Telegram на сервере (один раз; файл сессии с рабочего компьютера не копируйте —
одна сессия с двух адресов может быть отозвана Telegram):

```
docker compose -f docker-compose.prod.yml run --rm web python -m scripts.login
```

Перенос базы с рабочего компьютера:

```
docker compose exec -T postgres pg_dump -U telegram -Fc telegram_comments > telegram_comments.dump
scp telegram_comments.dump root@SERVER:/root/telegram-comments/
ssh root@SERVER 'cd /root/telegram-comments && docker compose -f docker-compose.prod.yml exec -T postgres \
  pg_restore -U telegram -d telegram_comments --clean --if-exists --no-owner < telegram_comments.dump'
```

Reverse proxy: пример для nginx с certbot — `deploy/nginx.conf.example`. Proxy обязан
передавать `X-Forwarded-Proto` и записывать в `X-Forwarded-For` только адрес посетителя
(`$remote_addr`): по нему считаются неверные пароли, и адрес, присланный самим посетителем,
позволил бы обойти ограничение.

## Переменные окружения

| Переменная | По умолчанию | Значение |
|---|---|---|
| `API_ID`, `API_HASH` | — | API-креды Telegram, обязательны |
| `DATABASE_URL` | — | Адрес PostgreSQL, `postgresql+asyncpg://user:password@host:5432/database`; обязателен |
| `APP_TIMEZONE` | `Asia/Almaty` | Пояс, в котором считаются часы и дни активности |
| `WEB_PASSWORD` | — | Пароль для входа на сайт; обязателен для режима `web` |
| `SESSION_DIR` | текущий каталог | Каталог файла сессии Telegram `my_session.session` |
| `DATA_DIR` | `data` | Каталог для весов моделей и журналов |
| `CHANNELS_PATH` | `channels.json` | Файл со списком каналов (его же дописывает `discover-channels`) |
| `LIMIT` | `1000` | Сообщений на источник для `collect`, `find-user` (история) и `discover-channels`; в `user-comments` не используется |
| `DISCOVER_TARGET` | `200` | На каком размере списка `discover-channels` прекращает поиск |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`, `POSTGRES_PORT` | `telegram`, `telegram`, `telegram_comments`, `5435` | Параметры контейнера PostgreSQL |
| `LOG_LEVEL` | `INFO` | Уровень логирования |

## Тесты

```
./scripts/tests.sh
```

Скрипт поднимает отдельную базу `postgres-test` и запускает все тесты в Docker. Локально:

```
docker compose --profile test up -d postgres-test
TEST_DATABASE_URL=postgresql+asyncpg://telegram:telegram@127.0.0.1:5436/telegram_comments_test \
    python -m pytest
```

Тесты из `tests/postgres` удаляют схему в своей базе, поэтому её имя обязано
оканчиваться на `_test`. Без `TEST_DATABASE_URL` они пропускаются.

## Каналы для сканирования

Реально обходятся те каналы, что перечислены в `channels.json`. Полный пул:

```
  rud01vb
  d_tyazhkun
  communistvrn
  vihod_est
  lenin_crew
  rev01ution_red
  spichka_media
  egoryakovleff
  vestnikburioriginals
  dialectic_club
  dharmazapisi
```
