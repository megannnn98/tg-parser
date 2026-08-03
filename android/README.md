# telegram-comments Android client

Минималистичная Android-обёртка над web UI проекта `telegram-comments`.
Приложение открывает FastAPI-бэкенд во встроенном `WebView` и не содержит собственной
логики: список каналов, скачивание комментариев, профиль пользователя — всё это
остаётся на стороне FastAPI. Единственная нативная часть — экран настроек, в котором
указывается URL бэкенда.

## Что нужно для работы

1. **Где-то работает FastAPI-сервер `telegram-comments`.** Это может быть:
   - тот же компьютер по Wi-Fi: `http://192.168.x.x:8000`,
   - домашний сервер / VPS: `https://comments.example.ru`,
   - эмулятор на самом телефоне (не рекомендуется для реального использования).
2. **Телефон и сервер видят друг друга по сети.** Для локального запуска FastAPI
   слушайте на `0.0.0.0`:
   ```sh
   WEB_PORT=8000 uvicorn web.app:app --host 0.0.0.0 --port 8000
   ```
   или через `scripts/run.sh web` (см. корневой `README.md`).

Приложение отправляет трафик по URL из настроек; cleartext HTTP разрешён, чтобы
работал адрес вида `http://192.168.x.x:8000` (см.
`app/src/main/res/xml/network_security_config.xml`). Для публичного деплоя
используйте HTTPS на стороне FastAPI или за reverse-proxy.

## Сборка APK

### Через Android Studio (проще)

1. `File → Open` → выберите каталог `android/`.
2. Дождитесь Gradle-sync. Android Studio сама подставит свой Gradle и нужные SDK.
3. `Run → Run 'app'` для отладочной сборки на подключённом устройстве, или
   `Build → Build Bundle(s)/APK(s) → Build APK(s)` для unsigned-debug APK.

### Из командной строки

Нужны JDK 17 (AGP 8.x официально требует 17 или 21; JDK 26 может не завестись) и
Android SDK с `platforms;android-35` и `build-tools;35.0.x`. Пропишите путь к SDK
в `android/local.properties`:

```
sdk.dir=/home/<user>/Android/Sdk
```

(`local.properties` в `.gitignore`.) Далее из каталога `android/`:

```sh
./gradlew assembleDebug
```

Готовый APK — `app/build/outputs/apk/debug/app-debug.apk`. Перенесите его на
телефон и установите (потребуется разрешение «установка из неизвестных
источников»).

## Тесты

Только pure-JVM unit-тесты на валидацию URL и хранилище настроек:

```sh
./gradlew test
```

Активити и сам `WebView` тестируются вручную: открыть настройки, ввести URL,
вернуться, убедиться, что сайт грузится.

## Структура

```
android/
  app/src/main/
    java/com/telegramcomments/app/
      MainActivity.kt                    WebView-хост, меню, обработка back-button
      SettingsActivity.kt                экран настройки URL бэкенда
      SettingsStore.kt                   обёртка над KeyValueStore
      KeyValueStore.kt                   интерфейс хранилища (для unit-тестов)
      SharedPreferencesKeyValueStore.kt  production-реализация
      UrlValidator.kt                    валидация + нормализация URL
    res/
      layout/                            activity_main (WebView + empty/error),
                                          activity_settings (поле URL + кнопка)
      menu/main.xml                      Reload, Settings
      values/                            strings, colors, themes
      xml/
        network_security_config.xml      cleartext для http://локальной-сети
        backup_rules.xml                 резервное копирование настроек
      mipmap-anydpi-v26/                 адаптивная иконка (API 26+)
      drawable/                          векторные ресурсы иконки
  app/src/test/                          UrlValidatorTest, SettingsStoreTest
  gradle/wrapper/                        gradle-wrapper.jar + .properties
  gradlew, gradlew.bat                   wrapper-скрипты
  build.gradle.kts, settings.gradle.kts  конфигурация Gradle
```

## Минимальные требования

- Android 8.0 (API 26) и выше.
- APK собран как debug (release-сборка без подписи не предусмотрена — добавьте
  signing config в `app/build.gradle.kts`, если нужен distribution).

## Что приложение НЕ делает

- Не хранит скачанные комментарии на телефоне. БД по-прежнему лежит на стороне
  FastAPI.
- Не запускает pyrogram-сессию внутри себя. Telegram-сессия остаётся на бэкенде.
- Не падает в Google Play без явной подписи release-APK и прохождения review.

## Известные ограничения

- При ошибке сети (бэкенд недоступен) приложение показывает экран с кнопкой
  «Обновить»; автоматического ретрая нет — намеренно, чтобы пользователь видел
  реальное состояние.
- Сертификаты только системные (см. `network_security_config.xml`). Самоподписанные
  сертификаты не поддерживаются — используйте валидный HTTPS или подключайтесь по
  HTTP в доверенной сети.
