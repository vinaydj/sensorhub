# SensorHub

Home IoT sensor dashboard running on Pi Zero 2 W (DietPi).

## Architecture
- **Nginx** on port 80 (static HTML + API proxy)
- **FastAPI** on port 8000 (SQLite backend)
- **systemd** service: `sensorhub`

## Dashboards
- `/` — DHT22 Temperature & Humidity
- `/air/` — Air Quality (AHT21 + ENS160)
- `/pir/` — PIR Occupancy

## Deployment
```bash
# Server
sudo cp server.py /opt/sensorhub/server.py

# Dashboards
sudo cp dht22_dashboard.html /var/www/sensorhub/index.html
sudo mkdir -p /var/www/sensorhub/air /var/www/sensorhub/pir
sudo cp air_dashboard.html /var/www/sensorhub/air/index.html
sudo cp pir_dashboard.html /var/www/sensorhub/pir/index.html

# Nginx
sudo cp nginx_sensorhub.conf /etc/nginx/sites-available/sensorhub
sudo ln -sf /etc/nginx/sites-available/sensorhub /etc/nginx/sites-enabled/

# Restart
sudo systemctl restart sensorhub nginx
```
