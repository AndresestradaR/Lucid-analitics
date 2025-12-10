"""
Utilidades: Encriptación de tokens y JWT
"""

from cryptography.fernet import Fernet
from passlib.context import CryptContext
from jose import JWTError, jwt
from datetime import datetime, timedelta
import os

# ========== CONFIGURACIÓN ==========

SECRET_KEY = os.getenv("SECRET_KEY", "tu-secret-key-muy-segura-cambiar-en-produccion")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 24 * 7  # 1 semana

# Clave para encriptar tokens de terceros (Meta, LucidBot)
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY", Fernet.generate_key().decode())
fernet = Fernet(ENCRYPTION_KEY.encode() if isinstance(ENCRYPTION_KEY, str) else ENCRYPTION_KEY)

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


# ========== PASSWORD HASHING ==========

def hash_password(password: str) -> str:
    """Hashear contraseña"""
    return pwd_context.hash(password)

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verificar contraseña"""
    return pwd_context.verify(plain_password, hashed_password)


# ========== JWT TOKENS ==========

def create_access_token(data: dict, expires_delta: timedelta = None) -> str:
    """Crear JWT token"""
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS))
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str) -> dict:
    """Decodificar JWT token"""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        return None


# ========== ENCRIPTACIÓN DE TOKENS DE TERCEROS ==========

def encrypt_token(token: str) -> str:
    """Encriptar token (para guardar en DB)"""
    return fernet.encrypt(token.encode()).decode()

def decrypt_token(encrypted_token: str) -> str:
    """Desencriptar token"""
    return fernet.decrypt(encrypted_token.encode()).decode()
