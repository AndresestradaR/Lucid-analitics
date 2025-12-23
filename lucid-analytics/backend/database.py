"""
Database models y configuración
Lucid Analytics - Meta Ads + LucidBot + Dropi
CON CACHE LOCAL PARA DROPI
"""

from sqlalchemy import create_engine, Column, Integer, BigInteger, String, DateTime, Float, Boolean, ForeignKey, Text, Numeric, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.pool import QueuePool
from datetime import datetime
import os

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./lucid_analytics.db")

# Ajuste para PostgreSQL en Railway
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Configuración del pool de conexiones OPTIMIZADA
# - pool_size: conexiones permanentes
# - max_overflow: conexiones adicionales temporales
# - pool_timeout: segundos para esperar una conexión libre
# - pool_recycle: reciclar conexiones después de N segundos (evita conexiones stale)
# - pool_pre_ping: verificar que la conexión esté viva antes de usarla
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,          # Aumentado de 5 a 10
    max_overflow=20,       # Aumentado de 10 a 20
    pool_timeout=10,       # Reducido de 30 a 10 (falla rápido)
    pool_recycle=300,      # Reciclar conexiones cada 5 minutos
    pool_pre_ping=True,    # Verificar conexiones antes de usar
)

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
    
    # API Key de Anthropic (encriptada) para El Cerebro
    anthropic_api_key_encrypted = Column(Text, nullable=True)
    
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
    
    # Token de API de LucidBot (encriptado) - método antiguo
    api_token_encrypted = Column(Text, nullable=True)
    account_id = Column(String(100))
    
    # JWT Token de sesión (encriptado) - método nuevo v2
    jwt_token_encrypted = Column(Text, nullable=True)
    page_id = Column(String(100), nullable=True)
    
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
    
    # Wallet cache - se actualiza en cada login exitoso
    cached_wallet_balance = Column(Numeric(12, 2), default=0)
    cached_wallet_updated_at = Column(DateTime, nullable=True)
    
    # Control de sincronización
    last_orders_sync = Column(DateTime, nullable=True)
    last_wallet_sync = Column(DateTime, nullable=True)
    sync_status = Column(String(50), default="pending")  # pending, syncing, completed, error
    
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


class Sale(Base):
    """Ventas registradas desde LucidBot"""
    __tablename__ = "sales"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    
    # Datos del contacto/venta
    contact_id = Column(String(100), index=True)
    ad_id = Column(String(100), index=True)
    phone = Column(String(50))
    name = Column(String(255))
    
    # Monto
    amount = Column(Float, default=0)
    currency = Column(String(10), default="COP")
    
    # Estado
    status = Column(String(50), default="completed")
    
    # Fechas
    sale_date = Column(DateTime, default=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)


class LucidbotContact(Base):
    """
    Contactos de LucidBot sincronizados localmente.
    Esto resuelve el límite de 100 contactos de la API.
    """
    __tablename__ = "lucidbot_contacts"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    
    # ID único de LucidBot (para evitar duplicados) - BIGINT porque puede ser muy grande
    lucidbot_id = Column(BigInteger, unique=True, index=True, nullable=False)
    
    # Datos del contacto
    full_name = Column(String(255))
    phone = Column(String(50), index=True)
    
    # Anuncio de Facebook
    ad_id = Column(String(100), index=True)
    
    # Datos de venta
    total_a_pagar = Column(Float, nullable=True)  # Si tiene valor, es una venta
    producto = Column(String(500), nullable=True)
    calificacion = Column(String(100), nullable=True)
    
    # JSON raw de LucidBot
    raw_data = Column(Text, nullable=True)
    
    # Fecha de creación en LucidBot (hora Colombia)
    contact_created_at = Column(DateTime, nullable=False, index=True)
    
    # Control de sincronización
    synced_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# ==================== NUEVAS TABLAS PARA CACHE DE DROPI ====================

class DropiOrder(Base):
    """
    Pedidos de Dropi sincronizados localmente.
    Permite consultas instantáneas sin llamar a la API.
    """
    __tablename__ = "dropi_orders"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    
    # ID único de Dropi
    dropi_order_id = Column(BigInteger, nullable=False, index=True)
    
    # Estado del pedido (ENTREGADO, DEVOLUCION, CANCELADO, etc.)
    status = Column(String(100), index=True)
    status_raw = Column(String(100))  # Estado original sin normalizar
    
    # Datos financieros
    total_order = Column(Numeric(12, 2), default=0)
    shipping_amount = Column(Numeric(12, 2), default=0)
    dropshipper_profit = Column(Numeric(12, 2), default=0)  # Lo que ganas
    
    # Datos del cliente
    customer_name = Column(String(255))
    customer_phone = Column(String(50))
    customer_city = Column(String(100))
    customer_state = Column(String(100))
    customer_address = Column(Text)
    
    # Datos de envío
    shipping_guide = Column(String(100))
    shipping_company = Column(String(100))
    rate_type = Column(String(50))  # CON RECAUDO, SIN RECAUDO, etc.
    
    # Productos (JSON serializado)
    products_json = Column(Text)
    
    # Fechas importantes de Dropi
    order_created_at = Column(DateTime, nullable=False, index=True)  # Fecha creación pedido
    order_updated_at = Column(DateTime)  # Última actualización en Dropi
    delivered_at = Column(DateTime)  # Fecha de entrega
    returned_at = Column(DateTime)  # Fecha de devolución
    
    # Cruce con wallet (para reconciliación)
    is_paid = Column(Boolean, default=False)  # ¿Ya te pagaron?
    paid_at = Column(DateTime)
    paid_amount = Column(Numeric(12, 2))
    wallet_transaction_id = Column(BigInteger)
    
    is_return_charged = Column(Boolean, default=False)  # ¿Ya te cobraron la devolución?
    return_charged_at = Column(DateTime)
    return_charged_amount = Column(Numeric(12, 2))
    
    # Metadatos de sincronización
    synced_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    raw_data = Column(Text)  # JSON completo de Dropi para debug
    
    # Índice compuesto único
    __table_args__ = (
        Index('idx_dropi_orders_user_dropi_id', 'user_id', 'dropi_order_id', unique=True),
        Index('idx_dropi_orders_user_created', 'user_id', 'order_created_at'),
        Index('idx_dropi_orders_user_status', 'user_id', 'status'),
    )


class DropiWalletHistory(Base):
    """
    Historial de wallet de Dropi sincronizado localmente.
    Permite cruce de datos para reconciliación.
    """
    __tablename__ = "dropi_wallet_history"
    
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    
    # ID único del movimiento en Dropi
    dropi_wallet_id = Column(BigInteger, nullable=False, index=True)
    
    # Tipo de movimiento
    movement_type = Column(String(50))  # ENTRADA, SALIDA
    description = Column(Text)
    
    # Montos
    amount = Column(Numeric(12, 2), default=0)
    balance_after = Column(Numeric(12, 2), default=0)  # Saldo después del movimiento
    
    # Referencia a orden (si aplica)
    order_id = Column(BigInteger, index=True)
    
    # Categorización (calculada)
    category = Column(String(50))  # ganancia_dropshipping, cobro_flete, retiro, recarga, otro
    
    # Fecha del movimiento
    movement_created_at = Column(DateTime, nullable=False, index=True)
    
    # Metadatos de sincronización
    synced_at = Column(DateTime, default=datetime.utcnow)
    raw_data = Column(Text)
    
    # Índice compuesto único
    __table_args__ = (
        Index('idx_dropi_wallet_user_dropi_id', 'user_id', 'dropi_wallet_id', unique=True),
        Index('idx_dropi_wallet_user_created', 'user_id', 'movement_created_at'),
        Index('idx_dropi_wallet_order', 'user_id', 'order_id'),
    )


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
