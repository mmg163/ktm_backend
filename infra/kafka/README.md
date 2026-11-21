# Kafka (ktm) — ansible

    cd infra/kafka && ansible-playbook playbook-kafka.yml          # нужен: community.general (ansible-galaxy collection install community.general)

Брокер 10.14.0.6 (KRaft, один узел), listener PLAINTEXT :9092, advertised 10.14.0.6.
Доступ к 9092 в UFW: WireGuard 10.14.0.0/24 и docker 172.18.0.0/16. Автосоздание топиков выключено.

| Топик | Партиций | Хранение |
|---|---|---|
| ktm.events.raw (все источники) | 9 | 7 суток |
| ktm.events.rejected | 3 | 30 суток |
| ktm.events.dlq | 3 | 30 суток |

Ключ сообщения: `<источник>:<ID события в источнике>` (задаёт продюсер; одинаковый ключ → одна партиция → порядок событий).
Прежние топики `ktm.events.raw.{skud,cv,1c-do}` заменены единым `ktm.events.raw`; если они уже созданы на брокере, роль их не удаляет — убрать вручную после проверки, что данных в них нет.
Менять число партиций у существующих топиков роль не умеет (только увеличение вручную).
