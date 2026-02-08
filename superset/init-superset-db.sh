#!/bin/bash
set -e

# Sử dụng biến POSTGRES_USER và POSTGRES_DB đã có sẵn (của airflow)
# để thực thi lệnh psql với tư cách là superuser
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Tạo user cho Superset
    CREATE USER superset WITH PASSWORD 'superset';
    
    -- Tạo database cho superset
    CREATE DATABASE superset;
    
    -- Cấp toàn bộ quyền cho user 'superset' trên database 'superset'
    GRANT ALL PRIVILEGES ON DATABASE superset TO superset;
EOSQL