# Инфраструктура вебхуков на сервере 194.67.122.251

## Сервер

- IP: `194.67.122.251`
- Домен: `194-67-122-251.regru.cloud`
- ОС: Ubuntu + Apache/2.4.58 + nginx (обратный прокси)

## Как устроен nginx

nginx слушает на **внутреннем** IP `192.168.0.207`, а не на `0.0.0.0`:
- HTTP: `192.168.0.207:80 default_server`
- HTTPS: `192.168.0.207:443 ssl default_server`

Основной конфиг домена:
```
/etc/nginx/vhosts/www-root/194-67-122-251.regru.cloud.conf
```

Этот файл включает location-блоки из:
```
/etc/nginx/vhosts-resources/194-67-122-251.regru.cloud/*.conf
```

**ВАЖНО:** `sites-enabled/` на этом сервере nginx НЕ использует для основного домена.
Файлы в `/etc/nginx/sites-available/` и `/etc/nginx/sites-enabled/` игнорируются
основным server-блоком. Добавлять новые маршруты нужно только через:
```
/etc/nginx/vhosts-resources/194-67-122-251.regru.cloud/webhook_proxy.conf
```

### Fallback

Всё, что не матчится явным `location`, уходит в `@fallback` → Apache на `127.0.0.1:8080`.
Именно поэтому незарегистрированные пути возвращают `Apache 404`.

## Файл маршрутизации вебхуков

```
/etc/nginx/vhosts-resources/194-67-122-251.regru.cloud/webhook_proxy.conf
```

Текущее содержимое:
```nginx
location = /wh_links_8081 {
    proxy_pass http://127.0.0.1:8081;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}

location ^~ /webhook {
    proxy_pass http://127.0.0.1:8000;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
}

location ^~ /health {
    proxy_pass http://127.0.0.1:8000;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
}
```

**ВАЖНО по типам location:**
- Для точных путей вебхука использовать `location = /path` (exact match).
  `^~` может не выиграть у вложенных `location /` в основном конфиге.
- Для префиксов (несколько путей с одного порта) можно `location ^~ /prefix`.

## Боты и порты

| Бот | Директория на сервере | Хост-порт | Внутренний порт | Путь вебхука |
|-----|-----------------------|-----------|-----------------|--------------|
| Старый (max-bot) | `~/max-bot` (или аналог) | 8000 | 8000 | `/webhook` |
| links_r (новый) | `~/links_max` | 8081 | 8080 | `/wh_links_8081` |

## SSL-сертификат

```
/var/www/httpd-cert/www-root/194-67-122-251.regru.cloud_le1.crt
/var/www/httpd-cert/www-root/194-67-122-251.regru.cloud_le1.key
```

## Чеклист при добавлении нового бота

1. Выбрать свободный хост-порт (проверить: `ss -tlnp | grep LISTEN`).
2. В `docker-compose.yml` нового бота: `ports: - "НОВЫЙ_ПОРТ:8080"`.
3. В `.env`: `WEBHOOK_PATH=/уникальный_путь`.
4. Добавить в `webhook_proxy.conf`:
   ```nginx
   location = /уникальный_путь {
       proxy_pass http://127.0.0.1:НОВЫЙ_ПОРТ;
       proxy_http_version 1.1;
       proxy_set_header Host $host;
       proxy_set_header X-Real-IP $remote_addr;
       proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
       proxy_set_header X-Forwarded-Proto $scheme;
   }
   ```
5. `sudo nginx -t && sudo systemctl reload nginx`
6. Проверить: `curl -k -i "https://194-67-122-251.regru.cloud/уникальный_путь"`
7. Поднять контейнер: `docker compose up -d --build`
8. Проверить логи: `docker compose logs -f bot`

## Диагностика

```bash
# Посмотреть все активные location и порты
sudo nginx -T 2>&1 | rg "server_name|listen|proxy_pass|location ="

# Проверить, что контейнер отвечает напрямую
curl -i "http://127.0.0.1:ПОРТ/путь"

# Проверить через nginx
curl -k -i "https://194-67-122-251.regru.cloud/путь"

# Логи ошибок nginx
sudo tail -f /var/www/httpd-logs/194-67-122-251.regru.cloud.error.log
```
