# RUN_WEBHOOK

## 1. Подготовка

1. Установите Docker и Docker Compose.
2. Скопируйте `.env.example` в `.env`.
3. Заполните переменные:
   - `BOT_TOKEN` — токен бота MAX.
   - `WEBHOOK_BASE_URL` — публичный HTTPS URL.
   - `WEBHOOK_PATH` — путь вебхука (по умолчанию `/max/webhook`).
   - `WEBHOOK_SECRET` — секрет, который MAX должен отправлять в заголовке `X-Max-Bot-Api-Secret`.
   - `ADMIN_USER_IDS` — список id админов через запятую.
   - `PERSONAL_DATA_POLICY_URL` — ссылка на правила обработки ПДн.

## 2. Запуск

```bash
docker compose up --build -d
```

Проверка:

```bash
docker compose logs -f bot
```

Health:

```bash
curl http://localhost:8080/health
```

## 3. Подключение webhook в MAX

Зарегистрируйте webhook URL:

`https://<ваш-домен><WEBHOOK_PATH>`

Пример:

`https://bot.example.ru/max/webhook`

При отправке событий MAX должен передавать заголовок:

`X-Max-Bot-Api-Secret: <WEBHOOK_SECRET>`

## 4. Первая настройка админки

1. Напишите боту `admin` с аккаунта, id которого есть в `ADMIN_USER_IDS`.
2. Создайте платформы.
3. Создайте офферы с 3 частями ссылки:
   - `prefix` (до `SUBID`)
   - `static_subid` (например `sub_id1=`)
   - `suffix` (после `SUBID`)
4. Создайте сценарии (`scenario7`, `scenario8`, ...), добавьте текст и картинку.
5. Создайте deep link на сценарий.
6. При необходимости добавьте обязательные каналы подписки.

## 5. Экспорт и рассылки

- Экспорт формируется в `.xlsx` через admin-команду `export`.
- Для рассылок есть режимы:
  - отправить сразу;
  - отправить позже (планировщик APScheduler).

## 6. Локальная отладка webhook

1. Поднимите проект: `docker compose up --build`.
2. Используйте туннель с HTTPS (например, ngrok/cloudflared) и укажите URL в `WEBHOOK_BASE_URL`.
3. Проверьте входящий webhook POST в `/max/webhook`.

## 7. Troubleshooting

- `403 Invalid webhook secret`:
  - проверьте соответствие `WEBHOOK_SECRET` и заголовка `X-Max-Bot-Api-Secret`.
- `SUBID limit reached for this offer`:
  - оффер выдал диапазон `0001..9999`, создайте новый оффер.
- Пользователь не может пройти шаг `Далее`:
  - проверьте обязательные каналы, права бота и корректность `chat_id`.
- Клавиатура после callback откатывается:
  - используйте безопасный ack (`message=None`, `notification=\" \"`) и не отправляйте пустой callback answer.
