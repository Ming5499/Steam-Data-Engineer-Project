# Steam Data Engineering Pipeline

## 1) Giới thiệu ngắn
Dự án này là một **pipeline data engineering** lấy dữ liệu từ nền tảng Steam (web/API), xử lý (transform), lưu trữ và hiển thị. Hỗ trợ cả dữ liệu **static** (metadata game) và **dynamic** (price, review, playercount). Mục tiêu là có một hệ thống gần thực tế để:

- Thu thập & stream sự kiện (Kafka)
- Lưu trữ quan hệ (MySQL) và document (MongoDB)
- Dùng dbt để build các model phân tích
- Dùng Debezium để capture thay đổi (CDC) và push về Kafka
- Notifier (Discord) khi có discount
- Giám sát bằng Prometheus + Grafana
- Orchestration bằng Prefect

---

## 2) Những service chính (local bằng docker‑compose)
- **MySQL** (3306 trong container; mapped `3307` host)
- **MongoDB** (27017 container; mapped `27018` host)
- **Kafka + Zookeeper** (kafka:9092 nội bộ; host bootstrap 29092)
- **Debezium / Kafka Connect** (8083)
- **Prometheus** (9090), **Pushgateway** (9091)
- **Grafana** (3000)
- **Prefect Server + Worker** (4200 + worker container)
- **Kafdrop** (9000) — UI kiểm tra topics/messages

(Chi tiết image & port có trong `docker-compose.yml`)

---

## 3) Luồng dữ liệu (tóm tắt)
1. Crawler (static/dynamic) -> producer -> **Kafka topics** (`steam-static`, `steam-dynamic`)
2. Consumer đọc topic -> upsert vào **MySQL** hoặc insert vào **MongoDB**
3. dbt chạy trên MySQL để tạo `game_price_history`, marts...
4. Debezium capture table `prices` -> emit CDC message -> topic `steamdb.steam_db.prices`
5. Consumer notifier lắng nghe CDC -> gửi message tới Discord khi phát hiện discount
6. Prometheus scrape metrics -> Grafana hiển thị

---

## 4) Chuẩn bị môi trường (Prerequisites)
- Docker & Docker Compose (v2+) cài trên máy dev/server
- Python 3.10+ (để chạy script dev nếu cần)
- MongoDB Compass (tuỳ chọn)
- Power BI Desktop (tuỳ chọn)
- Tạo file `.env` 

**Ví dụ .env**
```bash
MYSQL_HOST=localhost
MYSQL_PORT=3307
MYSQL_USER=root
MYSQL_PASSWORD=yourpassword
MYSQL_DB=steam_db
MONGO_URI=mongodb://localhost:27018
KAFKA_BOOTSTRAP=localhost:29092
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/xxx/yyy
PREFECT_API_URL=http://localhost:4200
```

---

## 5) Hướng dẫn triển khai nhanh (Local - docker-compose)
### 5.1 Start toàn bộ infra
```bash
git clone <repo>
cd steam-project
# Build + start services
docker-compose down -v
docker-compose up --build -d
```

### 5.2 Tạo Kafka topic (ví dụ tạo trước khi gửi message)
**Tạo topic `steam-static`**
```bash
docker exec -it steam-project_kafka_1 \
  kafka-topics --create --topic steam-static --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
```
**Tạo topic `steam-dynamic`**
```bash
docker exec -it steam-project_kafka_1 \
  kafka-topics --create --topic steam-dynamic --bootstrap-server kafka:9092 --partitions 3 --replication-factor 1
```

### 5.3 Kiểm tra kết nối Kafka / Broker
Từ host (nếu có kafka client):
```bash
kafka-broker-api-versions --bootstrap-server localhost:29092
```
Hoặc kiểm tra từ trong container:
```bash
docker exec -it steam-project_kafka_1 kafka-broker-api-versions --bootstrap-server kafka:9092
```

### 5.4 Kiểm tra topic/messages (consumer quick check)
```bash
docker exec -it steam-project_kafka_1 \
  kafka-console-consumer --bootstrap-server kafka:9092 --topic steam-static --from-beginning --max-messages 10
```

### 5.5 Chạy Kafka producers (ví dụ)
```bash
# Từ host (nếu môi trường python đã cài)
python kafka/producers/kafka_producer_info.py
python kafka/producers/kafka_producer_prices.py
python kafka/producers/kafka_producer_reviews.py
```
(hoặc chạy bên trong container worker nếu đã mount code vào image)

### 5.6 Chạy Kafka consumers
```bash
python kafka/consumers/kafka_consumer_info.py
python kafka/consumers/kafka_consumer_prices.py
python kafka/consumers/kafka_consumer_reviews.py
python kafka/consumers/discord_notifier_consumer.py
```
Consumers sẽ ghi vào MySQL/MongoDB theo mapping trong code.

---

## 6) Kiểm tra MongoDB (Compass / shell)
**Dùng mongo shell trong container**
```bash
docker exec -it steam-project_mongo_1 bash
# trong container
mongosh         # hoặc `mongo` nếu mongosh không có
use steam_db
db.reviews.find({"appid": "20"}).pretty()
```
Hoặc mở **MongoDB Compass**, kết nối vào `localhost:27018` và xem collection `steam.reviews`.

---

## 7) Prefect — deploy & chạy flows (chi tiết)
> (Phần này dựa trên cấu hình `prefect.yaml` trong repo)

### Bước 1: Khởi động Prefect + Worker
```bash
docker-compose down -v
docker-compose up --build -d
```
- Mở dashboard Prefect: http://localhost:4200
- Worker container tên `steam-project_prefect_worker_1` sẽ tự join vào `default-agent-pool` (nếu cấu hình đúng)

### Bước 2: Deploy flow (từ trong container worker)
```bash
docker exec -it steam-project_prefect_worker_1 bash
# trong container
prefect deploy --prefect-file prefect.yaml
```
Khi deploy bạn sẽ được chọn:
- Flow: `steam_price_pipeline` (ví dụ)
- Tên deployment: `steam-price` (vd)
- Work pool: `default-agent-pool`
- Remote storage: chọn `n` nếu code đã nằm trong container
- Schedule: chọn `n` (có thể add sau bằng `prefect deployment set-schedule`)

Kết quả: bạn sẽ thấy deployment hiển thị ở UI: http://localhost:4200/deployments

### Bước 3: Run deployment
Có 2 cách:
- **Trigger thủ công từ UI**: Deployments → chọn deployment → Run
- **Trigger từ CLI**:
```bash
docker exec -it steam-project_prefect_worker_1 \
  prefect deployment run steam-price/steam_price_pipeline
```

### Bước 4: Kiểm tra logs
```bash
docker logs -f steam-project_prefect_worker_1
```
Bạn sẽ thấy worker pick up flow và chạy các tasks (vd: `steam_tasks`).

---

## 8) Debezium connector (tạo qua API)
Tạo file cấu hình connector: `config/debezium_connector.json` (repo có thể chứa ví dụ). Sau đó khởi tạo connector:
```bash
curl -X POST -H "Content-Type: application/json" --data @config/debezium_connector.json http://localhost:8083/connectors
```
Nếu muốn chạy từ trong container connect:
```bash
docker exec -it steam-project_connect_1 bash
curl -X POST -H "Content-Type: application/json" --data @/usr/app/config/debezium_connector.json http://localhost:8083/connectors
```
Kiểm tra trạng thái connector:
```bash
curl http://localhost:8083/connectors/mysql-prices-connector/status
```

---

## 9) dbt (chạy trong container / trong worker image)
**Thư mục dbt** thường nằm tại `/usr/app/dbt/` trong image worker (tuỳ cấu hình). Các bước:
```bash
# vào container worker
docker exec -it steam-project_prefect_worker_1 bash
ls -la /usr/app/dbt/
cd /usr/app/dbt/dbt
# Kiểm tra kết nối
dbt debug
# Chạy models
dbt run
# Chạy tests
dbt test
```

---

## 10) Monitoring & Grafana
- Mở Prometheus: http://localhost:9090
- Mở Grafana: http://localhost:3000 (default admin/admin nếu chưa đổi) → Add data source Prometheus (`http://prometheus:9090` khi dùng trong Docker compose)
- Import dashboard JSON (nếu repo có `monitoring/grafana_dashboard.json`)

**Một vài metric debug**
```bash
# Check pipeline status (nếu workflow push metrics vào Pushgateway)
curl http://localhost:9091/metrics | grep steam_pipeline_status

# View current queues
curl http://localhost:9091/metrics | grep steam_queue_size

# Clear checkpoints (force full restart) - CẢNH BÁO: thao tác này xóa checkpoints local
rm -rf data/checkpoints/*
```

---

## 11) Debug nhanh & Troubleshooting
- **MySQL access**
  ```bash
  docker exec -it steam-project_mysql_1 mysql -u root -p
  ```
- **Kafka consumer lag / topics**: mở Kafdrop: http://localhost:9000
- **Prefect lỗi worker**: logs của container worker (xem ở trên)
- **Connector Debezium không start**: check connect logs `docker logs steam-project_connect_1`

---
