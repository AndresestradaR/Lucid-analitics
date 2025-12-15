"""
Lucid Analytics - Backend API
Dashboard de métricas para dropshipping COD
Integra Meta Ads + LucidBot + Dropi para calcular CPA real
"""

from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer
from contextlib import asynccontextmanager
import os
from dotenv import load_dotenv

from database import create_tables, get_db, engine
from routers import auth, meta, lucidbot, analytics, dropi, chat, sync

load_dotenv()


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
            lucidbot_id INTEGER UNIQUE NOT NULL,
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
        # Crear índices si no existen
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
        """
    ]
    
    with engine.connect() as conn:
        for migration in migrations:
            try:
                conn.execute(text(migration))
                conn.commit()
                print("✅ Migración ejecutada")
            except Exception as e:
                print(f"⚠️ Migración ya aplicada o error: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    print("🚀 Lucid Analytics iniciando...")
    create_tables()
    print("✅ Base de datos lista")
    run_migrations()
    print("✅ Migraciones completadas")
    yield
    # Shutdown
    print("👋 Lucid Analytics cerrando...")

app = FastAPI(
    title="Lucid Analytics API",
    description="Dashboard de métricas Meta Ads + LucidBot + Dropi para calcular CPA real",
    version="2.1.0",
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

@app.get("/")
async def root():
    return {
        "status": "ok",
        "service": "Lucid Analytics API",
        "version": "2.1.0",
        "features": ["Meta Ads", "LucidBot", "Dropi", "Chat IA", "Sync"],
        "docs": "/docs"
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
