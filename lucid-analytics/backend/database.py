"""
Database models y configuración
Lucid Analytics - Meta Ads + LucidBot + Dropi
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
    dropi_connection = relationship("DropiConnection", back_populates="user", uselist=False)


class InviteCode(Base):
    """Códigos de invitación para registro"""
    __tablename__ = "invite_codes"
    
    id = Column(Integer, primary_key=True, index=True)
    code = Column(String(20), unique=True, index=True, nullable=False)
    max_uses = Column(Integer, default=1)
    uses = Column(Integer, default=0)
    expires_at = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True)
    created_by = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class MetaAccount(Base):
    """Cuenta de Meta Ads conectada"""
    __tablename__ = "meta_accounts"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Datos de Meta
    meta_user_id = Column(String(100))
    account_id = Column(String(100), nullable=False)
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
    account_id = Column(String(100))
    
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relaciones
    user = relationship("User", back_populates="lucidbot_connection")


class DropiConnection(Base):
    """Conexión a Dropi"""
    __tablename__ = "dropi_connections"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), unique=True, nullable=False)
    
    # Credenciales (encriptadas)
    email_encrypted = Column(Text, nullable=False)
    password_encrypted = Column(Text, nullable=False)
    
    # País (gt, co, mx, etc.)
    country = Column(String(10), default="co")
    
    # Token temporal (se obtiene en cada sesión)
    current_token = Column(Text, nullable=True)
    token_expires_at = Column(DateTime, nullable=True)
    
    # Info de cuenta
    dropi_user_id = Column(String(100), nullable=True)
    dropi_user_name = Column(String(255), nullable=True)
    
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relaciones
    user = relationship("User", back_populates="dropi_connection")


class AdMetricsCache(Base):
    """Cache de métricas de anuncios"""
    __tablename__ = "ad_metrics_cache"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    account_id = Column(String(100), nullable=False)
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
    
    # Métricas de LucidBot
    leads_count = Column(Integer, default=0)
    sales_count = Column(Integer, default=0)
    revenue = Column(Float, default=0)
    
    # Métricas calculadas
    cpa = Column(Float, default=0)
    cpl = Column(Float, default=0)
    roas = Column(Float, default=0)
    
    # Fecha de los datos
    date = Column(DateTime, nullable=False)
    
    # Control
    last_synced_at = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)


class ChatHistory(Base):
    """Historial de chat con el Cerebro"""
    __tablename__ = "chat_history"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    role = Column(String(20), nullable=False)  # 'user' o 'assistant'
    content = Column(Text, nullable=False)
    
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
