"""
Lucid Analytics - Backend API
Dashboard de métricas para dropshipping COD
Integra Meta Ads + LucidBot + Dropi para calcular CPA real

CON SCHEDULER PARA SYNC AUTOMÁTICO
"""

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer
from contextlib import asynccontextmanager
import os
import asyncio
from dotenv import load_dotenv

from database import create_tables, get_db, engine
from routers import auth, meta, lucidbot, analytics, dropi, chat, sync, admin

# APScheduler para sync automático
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

load_dotenv()

# Scheduler global
scheduler = AsyncIOScheduler()


def run_migrations():
    """Ejecutar migraciones de base de datos"""
    from sqlalchemy import text
    
    migrations = [
        # Agregar columna para API key de Anthropic
        """
        DO $$ 
        BEGIN 
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'users' AND column_name = 'anthropic_api_key_encrypted'
            ) THEN 
                ALTER TABLE users ADD COLUMN anthropic_api_key_encrypted TEXT;
            END IF;
        END $$;
        """,
        # Crear tabla lucidbot_contacts si no existe
        """
        CREATE TABLE IF NOT EXISTS lucidbot_contacts (
            id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(id),
            lucidbot_id BIGINT NOT NULL,
            full_name VARCHAR(255),
            phone VARCHAR(50),
            ad_id VARCHAR(100),
            total_a_pagar FLOAT,
            producto VARCHAR(500),
            calificacion VARCHAR(100),
            contact_created_at TIMESTAMP NOT NULL,
            synced_at TIMESTAMP DEFAULT NOW(),
            updated_at TIMESTAMP DEFAULT NOW()
        );
        """,
        # Crear índice único compuesto para UPSERT - ESTE ES EL IMPORTANTE
        """
        DO $$
        BEGIN
            -- Primero eliminar el constraint único simple si existe
            IF EXISTS (
                SELECT 1 FROM pg_constraint 
                WHERE conname = 'lucidbot_contacts_lucidbot_id_key'
            ) THEN
                ALTER TABLE lucidbot_contacts DROP CONSTRAINT lucidbot_contacts_lucidbot_id_key;
            END IF;
            
            -- Crear índice único compuesto
            IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_lucidbot_contacts_user_lucidbot') THEN
                CREATE UNIQUE INDEX idx_lucidbot_contacts_user_lucidbot ON lucidbot_contacts(user_id, lucidbot_id);
            END IF;
        END $$;
        """,
        # Crear otros índices si no existen
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_lucidbot_contacts_user_id') THEN
                CREATE INDEX idx_lucidbot_contacts_user_id ON lucidbot_contacts(user_id);
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_lucidbot_contacts_ad_id') THEN
                CREATE INDEX idx_lucidbot_contacts_ad_id ON lucidbot_contacts(ad_id);
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_lucidbot_contacts_created_at') THEN
                CREATE INDEX idx_lucidbot_contacts_created_at ON lucidbot_contacts(contact_created_at);
            END IF;
        END $$;
        """,
        # Cambiar lucidbot_id de INTEGER a BIGINT para soportar IDs grandes
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'lucidbot_contacts' 
                AND column_name = 'lucidbot_id' 
                AND data_type = 'integer'
            ) THEN
                ALTER TABLE lucidbot_contacts ALTER COLUMN lucidbot_id TYPE BIGINT;
            END IF;
        END $$;
        """,
        
        # ==================== MIGRACIONES DROPI CACHE ====================
        
        # Agregar columnas a dropi_connections
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
        
        # Crear tabla dropi_orders
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
        
        # Índices para dropi_orders
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_dropi_orders_user_dropi_id') THEN
                CREATE UNIQUE INDEX idx_dropi_orders_user_dropi_id ON dropi_orders(user_id, dropi_order_id);
            END IF;
        END $$;
        """,
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_dropi_orders_user_created') THEN
                CREATE INDEX idx_dropi_orders_user_created ON dropi_orders(user_id, order_created_at);
            END IF;
        END $$;
        """,
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_dropi_orders_user_status') THEN
                CREATE INDEX idx_dropi_orders_user_status ON dropi_orders(user_id, status);
            END IF;
        END $$;
        """,
        
        # Crear tabla dropi_wallet_history
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
        
        # Índices para dropi_wallet_history
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_dropi_wallet_user_dropi_id') THEN
                CREATE UNIQUE INDEX idx_dropi_wallet_user_dropi_id ON dropi_wallet_history(user_id, dropi_wallet_id);
            END IF;
        END $$;
        """,
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_dropi_wallet_user_created') THEN
                CREATE INDEX idx_dropi_wallet_user_created ON dropi_wallet_history(user_id, movement_created_at);
            END IF;
        END $$;
        """,
        """
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_indexes WHERE indexname = 'idx_dropi_wallet_order') THEN
                CREATE INDEX idx_dropi_wallet_order ON dropi_wallet_history(user_id, order_id);
            END IF;
        END $$;
        """,
        
        # ==================== MIGRACIÓN 17: LIMPIAR raw_data PARA AHORRAR ESPACIO ====================
        # Esta migración limpia los datos raw_data existentes que consumen ~80% del espacio
        """
        UPDATE dropi_orders SET raw_data = NULL WHERE raw_data IS NOT NULL;
        """,
        """
        UPDATE dropi_wallet_history SET raw_data = NULL WHERE raw_data IS NOT NULL;
        """,
        # VACUUM para recuperar espacio en disco (solo en PostgreSQL)
        # Nota: VACUUM no puede ejecutarse dentro de una transacción, así que lo hacemos por separado
    ]
    
    with engine.connect() as conn:
        for i, migration in enumerate(migrations):
            try:
                conn.execute(text(migration))
                conn.commit()
                print(f"✅ Migración {i+1}/{len(migrations)} ejecutada")
            except Exception as e:
                print(f"⚠️ Migración {i+1} ya aplicada o error: {e}")
        
        # Ejecutar VACUUM por separado (requiere autocommit)
        try:
            conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.execute(text("VACUUM ANALYZE dropi_orders;"))
            conn.execute(text("VACUUM ANALYZE dropi_wallet_history;"))
            print("✅ VACUUM ejecutado - espacio recuperado")
        except Exception as e:
            print(f"⚠️ VACUUM no ejecutado (normal en algunas configuraciones): {e}")


async def scheduled_sync():
    """
    Función que se ejecuta cada 2 horas para sincronizar
    LucidBot y Dropi de todos los usuarios.
    """
    print("\n" + "="*50)
    print("🔄 [SCHEDULER] Iniciando sincronización automática...")
    print("="*50)
    
    try:
        # Importar funciones de sync
        from routers.sync_dropi import sync_all_dropi_users
        from routers.sync import sync_all_lucidbot_users
        
        # Sync Dropi
        print("\n📦 [SCHEDULER] Sincronizando Dropi...")
        try:
            dropi_results = await sync_all_dropi_users()
            print(f"✅ [SCHEDULER] Dropi: {len(dropi_results)} usuarios sincronizados")
            for r in dropi_results:
                if r.get("result", {}).get("success"):
                    print(f"   - {r['email']}: {r['result'].get('orders_synced', 0)} orders, {r['result'].get('wallet_synced', 0)} wallet")
                else:
                    print(f"   - {r['email']}: ERROR - {r['result'].get('error', 'Unknown')}")
        except Exception as e:
            print(f"❌ [SCHEDULER] Error Dropi: {e}")
        
        # Sync LucidBot
        print("\n📡 [SCHEDULER] Sincronizando LucidBot...")
        try:
            lucidbot_results = await sync_all_lucidbot_users()
            print(f"✅ [SCHEDULER] LucidBot: {len(lucidbot_results)} usuarios sincronizados")
            for r in lucidbot_results:
                if r.get("result", {}).get("success"):
                    print(f"   - {r['email']}: {r['result'].get('synced', 0)} contacts")
                else:
                    print(f"   - {r['email']}: ERROR - {r['result'].get('error', 'Unknown')}")
        except Exception as e:
            print(f"❌ [SCHEDULER] Error LucidBot: {e}")
        
        print("\n" + "="*50)
        print("✅ [SCHEDULER] Sincronización automática completada")
        print("="*50 + "\n")
        
    except Exception as e:
        print(f"\n❌ [SCHEDULER] Error general: {e}\n")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 Lucid Analytics iniciando...")
    create_tables()
    print("✅ Base de datos lista")
    run_migrations()
    print("✅ Migraciones completadas")
    
    # Iniciar scheduler
    scheduler.add_job(
        scheduled_sync,
        IntervalTrigger(hours=2),
        id="sync_all",
        name="Sync LucidBot + Dropi cada 2 horas",
        replace_existing=True
    )
    scheduler.start()
    print("✅ Scheduler iniciado - Sync cada 2 horas")
    
    # Ejecutar sync inicial después de 60 segundos (dar tiempo a migraciones y VACUUM)
    async def delayed_initial_sync():
        await asyncio.sleep(60)
        print("\n🚀 [STARTUP] Ejecutando sync inicial...")
        await scheduled_sync()
    
    asyncio.create_task(delayed_initial_sync())
    print("✅ Sync inicial programado para 60 segundos")
    
    yield
    
    # Shutdown
    print("👋 Lucid Analytics cerrando...")
    scheduler.shutdown()
    print("✅ Scheduler detenido")

app = FastAPI(
    title="Lucid Analytics API",
    description="Dashboard de métricas Meta Ads + LucidBot + Dropi para calcular CPA real",
    version="2.6.0",
    lifespan=lifespan
)

# CORS - permitir frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # En producción, especificar dominios
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Routers
app.include_router(auth.router, prefix="/api/auth", tags=["Autenticación"])
app.include_router(meta.router, prefix="/api/meta", tags=["Meta Ads"])
app.include_router(lucidbot.router, prefix="/api/lucidbot", tags=["LucidBot"])
app.include_router(dropi.router, prefix="/api/dropi", tags=["Dropi"])
app.include_router(analytics.router, prefix="/api/analytics", tags=["Analytics"])
app.include_router(chat.router, prefix="/api/chat", tags=["Chat IA"])
app.include_router(sync.router, prefix="/api/sync", tags=["Sincronización"])
app.include_router(admin.router, prefix="/api/admin", tags=["Administración"])

@app.get("/")
async def root():
    return {
        "status": "ok",
        "service": "Lucid Analytics API",
        "version": "2.6.0",
        "features": ["Meta Ads", "LucidBot", "Dropi", "Chat IA", "Sync", "Admin", "Scheduler"],
        "scheduler": "running" if scheduler.running else "stopped",
        "docs": "/docs"
    }

@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "scheduler": "running" if scheduler.running else "stopped",
        "jobs": len(scheduler.get_jobs())
    }


@app.post("/api/cron/sync-all")
async def cron_sync_all():
    """
    Endpoint para disparar sync manualmente o desde Railway Cron.
    También se puede usar como backup si el scheduler falla.
    """
    asyncio.create_task(scheduled_sync())
    return {
        "status": "started",
        "message": "Sincronización iniciada en background"
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
