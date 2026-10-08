# telegram-comments

Собирает комментарии из discussion-чатов, привязанных к Telegram-каналам, и складывает их в SQLite.

## Локальные эмбеддинги и сравнение авторов

На странице пользователя раздел «Похожие высказывания авторов» создаёт эмбеддинги
самих комментариев из локальных SQLite-баз всех собранных авторов. Нажмите
«Создать эмбеддинги и сравнить». Панель показывает чтение истории, создание векторов
и сравнение авторов, текущую операцию, количество обработанных текстов и кеш.

Веб-анализ использует только локальную `intfloat/multilingual-e5-base`, ревизия
`d128750597153bb5987e10b1c3493a34e5a4502a`. Ключ OpenRouter для этого раздела
не нужен, комментарии во внешний API не отправляются. При первом запуске веса
скачиваются из Hugging Face в `DATA_DIR/position-model-cache`; затем модель работает
на CPU или доступной CUDA. Первый запуск требует сети для скачивания весов.
После обновления `requirements.txt` пересоберите образ командой
`docker build -f ci/Dockerfile -t telegram-parser .`, чтобы установить зависимости
локального анализа, включая NumPy, PyTorch и Transformers.

Для каждого уникального непустого текста сохраняется нормализованный вектор из
768 чисел. Длинные комментарии обрабатываются частями до 512 токенов с префиксом
`query:`; векторы частей усредняются с весом по длине и нормализуются по L2.
Эмбеддинги хранятся как JSON-массивы в `DATA_DIR/position-analysis.sqlite3`, таблица
`cache`, namespace `comment-embeddings:<версия модели>`. Исходные БД не меняются.
Модель, размерность, путь и число векторов показаны в раскрывающемся блоке панели.

Рейтинг строится по косинусному сходству нормализованных средних векторов уникальных
комментариев каждого автора. Повторение одного текста не увеличивает его вес.
Показываются десять ближайших авторов и до пяти пар похожих высказываний из 32
наиболее типичных комментариев каждого автора. Значение лежит между −1 и 1;
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

1. Положите API-креды Telegram в `.env` (и в `.env.docker` для контейнера):

```
API_ID=1234567
API_HASH=0123456789abcdef0123456789abcdef
LIMIT=1000
DB_PATH=data/app.db
LOG_LEVEL=INFO
```

2. Соберите образ:

```
docker build -f ci/Dockerfile -t telegram-parser .
```

3. Один раз войдите интерактивно — Pyrogram спрашивает номер телефона и код, а
   `scripts/run.sh` запускается без TTY, поэтому первый вход нужно сделать вручную:

```
docker run --rm -it --user "$(id -u):$(id -g)" --env-file .env.docker -v "$PWD:/app" telegram-parser collect
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

Пишет только его комментарии в отдельную БД — по одному файлу на пользователя. Имя файла
строится из данных, которые вернул Telegram, и всегда заканчивается на `tg_id`:

| Что известно о пользователе | Имя файла |
|---|---|
| есть `@username` | `<DATA_DIR>/hryukalo_555123456.db` |
| ника нет, есть имя «Хрюкало Офф» | `<DATA_DIR>/хрюкало_офф_555123456.db` |
| ни ника, ни имени | `<DATA_DIR>/555123456.db` |

Ник в приоритете над отображаемым именем. Имя приводится к нижнему регистру, пробелы
заменяются на `_`, всё, кроме букв (любого алфавита), цифр и `_`, отбрасывается — эмодзи и
пунктуация в путь не попадают.

Принимает и `@username`, и числовой `tg_id` — оба варианта дают один и тот же файл, потому
что имя берётся из ответа Telegram, а не из того, что вы набрали.

Фильтрация выполняется на стороне Telegram (`search_messages(from_user=...)`), поэтому
забирается вся история пользователя в каждом discussion-чате — `LIMIT` в этом режиме не
применяется. Повторные запуски идемпотентны: `UNIQUE(channel, message_id)` не даёт записать
уже существующий комментарий второй раз.

### Пример: собрать @Mega_Palez

```
./scripts/run.sh user-comments @Mega_Palez
```

Username приводится к нижнему регистру и обрезается до `[a-z0-9_]`, поэтому этот запуск
пишет в `data/mega_palez_<tg_id>.db`. Ожидаемый вывод (id будет реальный, тот, что вернёт
Telegram):

```
2026-08-01 10:15:02,113 | INFO     | user_collector | Resolved Mega_Palez -> tg_id=123456789, username=Mega_Palez
2026-08-01 10:15:03,455 | INFO     | user_collector | [rud01vb] fetched 37, new 37
2026-08-01 10:15:04,802 | INFO     | user_collector | [d_tyazhkun] fetched 12, new 12
2026-08-01 10:15:04,806 | INFO     | measure_time   | [collect_user_comments] took 3.104s
2026-08-01 10:15:04,807 | INFO     | main           | Saved 49 new comments of Mega_Palez to data/mega_palez_123456789.db
```

Числовой `tg_id` можно узнать из строки `Resolved ...` — он же входит в имя файла.

Посмотреть, что записалось:

```
sqlite3 data/mega_palez_123456789.db \
  "SELECT channel, COUNT(*) FROM user_messages GROUP BY channel ORDER BY 2 DESC;"

sqlite3 data/mega_palez_123456789.db \
  "SELECT date, text FROM user_messages ORDER BY date DESC LIMIT 5;"
```

Запустите ту же команду повторно, чтобы дозабрать только новые комментарии — по каждому
каналу будет `new 0`, а в последней строке `Saved 0 new comments`:

```
./scripts/run.sh user-comments @Mega_Palez
```

Когда id известен, можно обращаться к тому же файлу по id — так же работает и для
пользователей, у которых username нет вообще:

```
./scripts/run.sh user-comments 123456789        # -> data/mega_palez_123456789.db
```

Каналы без привязанного обсуждения логируются и пропускаются
(`Channel <name> has no linked discussion`). Канал, упавший по любой другой причине
(неверный username, нет доступа, длинный `FloodWait`), логируется с трейсбеком и
пропускается, а проход продолжается; в конце запуск сообщает `N of M channels failed`.
Мёртвая сессия (отозвана, auth key не зарегистрирован, аккаунт деактивирован) наоборот
прерывает проход — в логе указано, сколько строк уже успело закоммититься.

Задайте `USER_DB_PATH`, чтобы писать в один фиксированный файл, игнорируя имя по пользователю.

## Веб-профиль пользователя

Веб-интерфейс ничего не скачивает из Telegram и ничего не пишет в SQLite. Он только
читает уже готовые базы пользователей из `DATA_DIR`.

Сначала соберите комментарии нужного пользователя:

```
./scripts/run.sh user-comments @Mega_Palez
```

После успешного сбора в `data/` появится файл вида
`mega_palez_<tg_id>.db`. Затем соберите фронтенд (один раз и после каждого
изменения в `frontend/`; нужен Node.js 22.13+, на Arch: `sudo pacman -S nodejs npm`)
и запустите web UI:

```
(cd frontend && npm ci && npm run build)
./scripts/run.sh web
```

FastAPI отдаёт сборку из `frontend/dist` (другой путь можно задать в `FRONTEND_DIST`), а
JSON API живёт под `/api/v1` (схема — `http://localhost:8000/docs`). Без сборки любая
страница отвечает `503` с подсказкой, как её получить. Вместо локальной сборки можно
скачать артефакт `frontend-dist` последнего прогона GitHub Actions и распаковать его в
`frontend/dist`.

Откройте в браузере:

```
http://localhost:8000
```

На главной странице будет список всех найденных user DB. Нажмите на нужного пользователя,
чтобы открыть профиль с ником, `tg_id`, количеством сообщений, количеством каналов,
круговой диаграммой и таблицей `канал -> сообщения -> доля`.

Если порт `8000` занят, поменяйте host-порт:

```
WEB_PORT=8010 ./scripts/run.sh web
```

Тогда адрес будет:

```
http://localhost:8010
```

Docker-режим берёт `DATA_DIR` из `.env.docker`. Если базы лежат не в `data/`, задайте там
нужный каталог, например:

```
DATA_DIR=data
```

Без Docker можно запустить так:

```
DATA_DIR=data .venv/bin/python -m uvicorn web.app:app --host 127.0.0.1 --port 8000
```

Для другого локального порта:

```
DATA_DIR=data .venv/bin/python -m uvicorn web.app:app --host 127.0.0.1 --port 8010
```

Быстрая проверка из терминала:

```
curl -sS http://127.0.0.1:8000/api/v1/profiles
```

Если страница пустая, проверьте:

```
find data -maxdepth 1 -name "*.db" -print
sqlite3 data/mega_palez_<tg_id>.db \
  "SELECT channel, COUNT(*) FROM user_messages GROUP BY channel ORDER BY 2 DESC;"
```

`app.db` в списке профилей не показывается: это общая база режима `collect`, а web UI
ищет только таблицу `user_messages`.

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

## Запуск остальных режимов

```
./scripts/run.sh collect
```

## Данные

`data/app.db` (режим `collect`):

```
users(tg_id PK, username)
channels(name PK)
messages(id PK, user -> users.tg_id, channel -> channels.name, text, date)
```

БД одного пользователя (режим `user-comments`):

```
user_messages(id PK, tg_id, username, channel, message_id, text, date,
              UNIQUE(channel, message_id))
```

В обеих текст хранится после `normalize()` — Unicode NFKC плюс приведение к нижнему регистру.

## Переменные окружения

| Переменная | По умолчанию | Значение |
|---|---|---|
| `API_ID`, `API_HASH` | — | API-креды Telegram, обязательны |
| `DATA_DIR` | `data` | Каталог для баз данных |
| `DB_PATH` | `<DATA_DIR>/app.db` | База для `collect` |
| `USER_DB_PATH` | не задана | Не задана: `user-comments` называет файл по пользователю. Задана: используется ровно этот файл |
| `CHANNELS_PATH` | `channels.json` | Файл со списком каналов (его же дописывает `discover-channels`) |
| `LIMIT` | `1000` | Сообщений на источник для `collect`, `find-user` (история) и `discover-channels`; в `user-comments` не используется |
| `DISCOVER_TARGET` | `200` | На каком размере списка `discover-channels` прекращает поиск |
| `LOG_LEVEL` | `INFO` | Уровень логирования |

## Тесты

```
./scripts/tests.sh
```

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
