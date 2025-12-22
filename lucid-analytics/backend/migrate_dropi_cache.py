"""
Migración para crear tablas de cache de Dropi.
Ejecutar UNA VEZ después de deploy:

    python migrate_dropi_cache.py
"""

import os
from sqlalchemy import create_engine, text

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./lucid_analytics.db")

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)

MIGRATIONS = [
    # 1. Tabla dropi_orders
    """
    CREATE TABLE IF NOT EXISTS dropi_orders (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(id),
        dropi_order_id BIGINT NOT NULL,
        status VARCHAR(100),
        status_raw VARCHAR(100),
        total_order NUMERIC(12, 2) DEFAULT 0,
        shipping_amount NUMERIC(12, 2) DEFAULT 0,
        dropshipper_profit NUMERIC(12, 2) DEFAULT 0,
        customer_name VARCHAR(255),
        customer_phone VARCHAR(50),
        customer_city VARCHAR(100),
        customer_state VARCHAR(100),
        customer_address TEXT,
        shipping_guide VARCHAR(100),
        shipping_company VARCHAR(100),
        rate_type VARCHAR(50),
        products_json TEXT,
        order_created_at TIMESTAMP NOT NULL,
        order_updated_at TIMESTAMP,
        delivered_at TIMESTAMP,
        returned_at TIMESTAMP,
        is_paid BOOLEAN DEFAULT FALSE,
        paid_at TIMESTAMP,
        paid_amount NUMERIC(12, 2),
        wallet_transaction_id BIGINT,
        is_return_charged BOOLEAN DEFAULT FALSE,
        return_charged_at TIMESTAMP,
        return_charged_amount NUMERIC(12, 2),
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        raw_data TEXT
    );
    """,
    
    # 2. Índices para dropi_orders
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_dropi_orders_user_dropi_id 
    ON dropi_orders(user_id, dropi_order_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_dropi_orders_user_created 
    ON dropi_orders(user_id, order_created_at);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_dropi_orders_user_status 
    ON dropi_orders(user_id, status);
    """,
    
    # 3. Tabla dropi_wallet_history
    """
    CREATE TABLE IF NOT EXISTS dropi_wallet_history (
        id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES users(id),
        dropi_wallet_id BIGINT NOT NULL,
        movement_type VARCHAR(50),
        description TEXT,
        amount NUMERIC(12, 2) DEFAULT 0,
        balance_after NUMERIC(12, 2) DEFAULT 0,
        order_id BIGINT,
        category VARCHAR(50),
        movement_created_at TIMESTAMP NOT NULL,
        synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        raw_data TEXT
    );
    """,
    
    # 4. Índices para dropi_wallet_history
    """
    CREATE UNIQUE INDEX IF NOT EXISTS idx_dropi_wallet_user_dropi_id 
    ON dropi_wallet_history(user_id, dropi_wallet_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_dropi_wallet_user_created 
    ON dropi_wallet_history(user_id, movement_created_at);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_dropi_wallet_order 
    ON dropi_wallet_history(user_id, order_id);
    """,
    
    # 5. Agregar columnas a dropi_connections si no existen
    """
    DO $$ 
    BEGIN 
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name='dropi_connections' AND column_name='last_orders_sync') THEN
            ALTER TABLE dropi_connections ADD COLUMN last_orders_sync TIMESTAMP;
        END IF;
    END $$;
    """,
    """
    DO $$ 
    BEGIN 
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name='dropi_connections' AND column_name='last_wallet_sync') THEN
            ALTER TABLE dropi_connections ADD COLUMN last_wallet_sync TIMESTAMP;
        END IF;
    END $$;
    """,
    """
    DO $$ 
    BEGIN 
        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                       WHERE table_name='dropi_connections' AND column_name='sync_status') THEN
            ALTER TABLE dropi_connections ADD COLUMN sync_status VARCHAR(50) DEFAULT 'pending';
        END IF;
    END $$;
    """,
]


def run_migrations():
    print("Running Dropi cache migrations...")
    
    with engine.connect() as conn:
        for i, sql in enumerate(MIGRATIONS, 1):
            try:
                conn.execute(text(sql))
                conn.commit()
                print(f"  [{i}/{len(MIGRATIONS)}] OK")
            except Exception as e:
                print(f"  [{i}/{len(MIGRATIONS)}] Error (may be OK if already exists): {e}")
    
    print("\nMigrations completed!")
    print("\nNow you can sync Dropi data via:")
    print("  - Admin panel: POST /api/admin/dropi/sync-all")
    print("  - Or wait for automatic sync (every 2 hours)")


if __name__ == "__main__":
    run_migrations()
