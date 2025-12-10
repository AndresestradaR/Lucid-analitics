"""
Database models y configuración
"""

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Boolean, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./lucid_analytics.db")

# Ajuste para PostgreSQL en Railway
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ========== MODELOS ==========

class User(Base):
    """Usuario de la plataforma"""
    __tablename__ = "users"
    
    id = Column(Integer, primary_key=True, index=True)
    email = Column(String(255), unique=True, index=True, nullable=False)
    password_hash = Column(String(255), nullable=False)
    name = Column(String(255))
    is_active = Column(Boolean, default=True)
    is_admin = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relaciones
    meta_accounts = relationship("MetaAccount", back_populates="user")
    lucidbot_connection = relationship("LucidbotConnection", back_populates="user", uselist=False)


class MetaAccount(Base):
    """Cuenta de Meta Ads conectada"""
    __tablename__ = "meta_accounts"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Datos de Meta
    meta_user_id = Column(String(100))  # ID del usuario en Meta
    account_id = Column(String(100), nullable=False)  # Ad Account ID
    account_name = Column(String(255))
    
    # Token (encriptado)
    access_token_encrypted = Column(Text)
    token_expires_at = Column(DateTime)
    
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relaciones
    user = relationship("User", back_populates="meta_accounts")


class LucidbotConnection(Base):
    """Conexión a LucidBot"""
    __tablename__ = "lucidbot_connections"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True, nullable=False)
    
    # Token de LucidBot (encriptado)
    api_token_encrypted = Column(Text, nullable=False)
    account_id = Column(String(100))  # ID de cuenta en LucidBot
    
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relaciones
    user = relationship("User", back_populates="lucidbot_connection")


class AdMetricsCache(Base):
    """Cache de métricas de anuncios (para no llamar Meta cada vez)"""
    __tablename__ = "ad_metrics_cache"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    account_id = Column(String(100), nullable=False)  # Meta Ad Account
    ad_id = Column(String(100), nullable=False, index=True)
    
    # Métricas de Meta
    ad_name = Column(String(500))
    spend = Column(Float, default=0)
    impressions = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    ctr = Column(Float, default=0)
    cpm = Column(Float, default=0)
    cpc = Column(Float, default=0)
    reach = Column(Integer, default=0)
    
    # Métricas de LucidBot (calculadas)
    leads_count = Column(Integer, default=0)
    sales_count = Column(Integer, default=0)
    revenue = Column(Float, default=0)
    
    # Métricas calculadas
    cpa = Column(Float, default=0)  # Costo por adquisición (venta)
    cpl = Column(Float, default=0)  # Costo por lead
    roas = Column(Float, default=0)  # Return on Ad Spend
    
    # Fecha de los datos
    date = Column(DateTime, nullable=False)
    
    # Control
    last_synced_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)


class Sale(Base):
    """Ventas detectadas (de LucidBot)"""
    __tablename__ = "sales"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Datos del contacto
    contact_id = Column(String(100), nullable=False)
    contact_name = Column(String(255))
    contact_phone = Column(String(50))
    
    # Datos del anuncio
    ad_id = Column(String(100), index=True)
    
    # Datos de la venta
    amount = Column(Float, nullable=False)
    product_name = Column(String(255))
    
    # Fechas
    sale_date = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


# ========== FUNCIONES ==========

def create_tables():
    """Crear todas las tablas"""
    Base.metadata.create_all(bind=engine)

def get_db():
    """Dependency para obtener sesión de DB"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
