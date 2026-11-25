# Kafka (ktm) — ansible

    cd infra/kafka && ansible-playbook playbook-kafka.yml          # нужен: community.general (ansible-galaxy collection install community.general)

Брокер 10.14.0.6 (KRaft, один узел), listener PLAINTEXT :9092, advertised 10.14.0.6.
Доступ к 9092 в UFW: WireGuard 10.14.0.0/24 и docker 172.18.0.0/16. Автосоздание топиков выключено.

| Топик | Партиций | Хранение |
|---|---|---|
| ktm.events.raw.skud | 3 | 7 суток |
| ktm.events.raw.cv | 3 | 7 суток |
| ktm.events.raw.1c-do | 3 | 7 суток |
| ktm.events.rejected | 3 | 30 суток |
| ktm.events.dlq | 3 | 30 суток |

Ключ сообщения: `<источник>:<ID события в источнике>` (задаёт продюсер; одинаковый ключ → одна партиция → порядок событий).
Каждый источник публикует события в собственный `ktm.events.raw.<source>` с тремя партициями. Роль не удаляет прежний общий `ktm.events.raw`: его данные и подписки необходимо отдельно учесть при переходе.
Менять число партиций у существующих топиков роль не умеет (только увеличение вручную).
