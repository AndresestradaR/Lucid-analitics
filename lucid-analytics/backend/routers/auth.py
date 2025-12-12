"""
Router de autenticación con códigos de invitación
"""

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr
from typing import Optional
from datetime import datetime, timedelta
import secrets
import string

from database import get_db, User, InviteCode
from utils import hash_password, verify_password, create_access_token, decode_token

router = APIRouter()
security = HTTPBearer()


# ========== SCHEMAS ==========

class UserCreate(BaseModel):
    email: EmailStr
    password: str
    name: Optional[str] = None
    invite_code: str


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class UserResponse(BaseModel):
    id: int
    email: str
    name: Optional[str]
    is_active: bool
    is_admin: bool
    created_at: datetime
    
    class Config:
        from_attributes = True


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


class InviteCodeCreate(BaseModel):
    max_uses: int = 1
    expires_in_days: int = 7


class InviteCodeResponse(BaseModel):
    id: int
    code: str
    max_uses: int
    uses: int
    expires_at: Optional[datetime]
    is_active: bool
    created_at: datetime
    
    class Config:
        from_attributes = True


# ========== DEPENDENCY: OBTENER USUARIO ACTUAL ==========

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> User:
    """Obtener usuario actual del token"""
    token = credentials.credentials
    payload = decode_token(token)
    
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido o expirado"
        )
    
    user_id = payload.get("user_id")
    user = db.query(User).filter(User.id == user_id).first()
    
    if not user or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario no encontrado o inactivo"
        )
    
    return user


async def get_admin_user(
    current_user: User = Depends(get_current_user)
) -> User:
    """Verificar que el usuario es admin"""
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Se requieren permisos de administrador"
        )
    return current_user


# ========== HELPERS ==========

def generate_invite_code(length: int = 8) -> str:
    """Genera un código de invitación aleatorio"""
    chars = string.ascii_uppercase + string.digits
    # Excluir caracteres confusos
    chars = chars.replace('O', '').replace('0', '').replace('I', '').replace('1', '').replace('L', '')
    return ''.join(secrets.choice(chars) for _ in range(length))


# ========== ENDPOINTS DE AUTH ==========

@router.post("/register", response_model=TokenResponse)
async def register(user_data: UserCreate, db: Session = Depends(get_db)):
    """Registrar nuevo usuario con código de invitación"""
    
    # Verificar código de invitación
    invite = db.query(InviteCode).filter(
        InviteCode.code == user_data.invite_code.upper(),
        InviteCode.is_active == True
    ).first()
    
    if not invite:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Código de invitación inválido"
        )
    
    # Verificar si no ha expirado
    if invite.expires_at and invite.expires_at < datetime.utcnow():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Código de invitación expirado"
        )
    
    # Verificar usos disponibles
    if invite.uses >= invite.max_uses:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Código de invitación agotado"
        )
    
    # Verificar si email ya existe
    existing = db.query(User).filter(User.email == user_data.email).first()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El email ya está registrado"
        )
    
    # Crear usuario
    user = User(
        email=user_data.email,
        password_hash=hash_password(user_data.password),
        name=user_data.name
    )
    db.add(user)
    
    # Incrementar uso del código
    invite.uses += 1
    
    db.commit()
    db.refresh(user)
    
    # Generar token
    token = create_access_token({"user_id": user.id, "email": user.email})
    
    return TokenResponse(
        access_token=token,
        user=UserResponse.model_validate(user)
    )


@router.post("/login", response_model=TokenResponse)
async def login(credentials: UserLogin, db: Session = Depends(get_db)):
    """Iniciar sesión"""
    
    user = db.query(User).filter(User.email == credentials.email).first()
    
    if not user or not verify_password(credentials.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Email o contraseña incorrectos"
        )
    
    if not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario inactivo"
        )
    
    # Generar token
    token = create_access_token({"user_id": user.id, "email": user.email})
    
    return TokenResponse(
        access_token=token,
        user=UserResponse.model_validate(user)
    )


@router.get("/me", response_model=UserResponse)
async def get_me(current_user: User = Depends(get_current_user)):
    """Obtener usuario actual"""
    return UserResponse.model_validate(current_user)


@router.put("/profile", response_model=UserResponse)
async def update_profile(
    name: Optional[str] = None,
    email: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Actualizar perfil"""
    if name:
        current_user.name = name
    if email and email != current_user.email:
        # Verificar que no existe
        existing = db.query(User).filter(User.email == email, User.id != current_user.id).first()
        if existing:
            raise HTTPException(status_code=400, detail="Email ya en uso")
        current_user.email = email
    
    db.commit()
    db.refresh(current_user)
    
    return UserResponse.model_validate(current_user)


@router.put("/password")
async def change_password(
    current_password: str,
    new_password: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Cambiar contraseña"""
    if not verify_password(current_password, current_user.password_hash):
        raise HTTPException(status_code=400, detail="Contraseña actual incorrecta")
    
    if len(new_password) < 8:
        raise HTTPException(status_code=400, detail="La contraseña debe tener al menos 8 caracteres")
    
    current_user.password_hash = hash_password(new_password)
    db.commit()
    
    return {"message": "Contraseña actualizada"}


# ========== ENDPOINTS DE ADMIN ==========

@router.post("/admin/invite-codes", response_model=InviteCodeResponse)
async def create_invite_code(
    data: InviteCodeCreate,
    current_user: User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """Crear código de invitación (solo admin)"""
    
    # Generar código único
    code = generate_invite_code()
    while db.query(InviteCode).filter(InviteCode.code == code).first():
        code = generate_invite_code()
    
    expires_at = datetime.utcnow() + timedelta(days=data.expires_in_days) if data.expires_in_days > 0 else None
    
    invite = InviteCode(
        code=code,
        max_uses=data.max_uses,
        expires_at=expires_at,
        created_by=current_user.id
    )
    db.add(invite)
    db.commit()
    db.refresh(invite)
    
    return InviteCodeResponse.model_validate(invite)


@router.get("/admin/invite-codes")
async def list_invite_codes(
    current_user: User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """Listar códigos de invitación (solo admin)"""
    codes = db.query(InviteCode).order_by(InviteCode.created_at.desc()).all()
    
    return {
        "codes": [
            {
                "id": c.id,
                "code": c.code,
                "max_uses": c.max_uses,
                "uses": c.uses,
                "expires_at": c.expires_at.isoformat() if c.expires_at else None,
                "is_active": c.is_active and (c.expires_at is None or c.expires_at > datetime.utcnow()) and c.uses < c.max_uses,
                "created_at": c.created_at.isoformat()
            }
            for c in codes
        ]
    }


@router.delete("/admin/invite-codes/{code_id}")
async def delete_invite_code(
    code_id: int,
    current_user: User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """Eliminar código de invitación (solo admin)"""
    invite = db.query(InviteCode).filter(InviteCode.id == code_id).first()
    if not invite:
        raise HTTPException(status_code=404, detail="Código no encontrado")
    
    db.delete(invite)
    db.commit()
    
    return {"message": "Código eliminado"}


@router.get("/admin/users")
async def list_users(
    current_user: User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """Listar usuarios (solo admin)"""
    users = db.query(User).order_by(User.created_at.desc()).all()
    
    return {
        "users": [
            {
                "id": u.id,
                "email": u.email,
                "name": u.name,
                "is_active": u.is_active,
                "is_admin": u.is_admin,
                "created_at": u.created_at.isoformat()
            }
            for u in users
        ]
    }


@router.put("/admin/users/{user_id}")
async def update_user(
    user_id: int,
    is_active: Optional[bool] = None,
    is_admin: Optional[bool] = None,
    current_user: User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """Actualizar usuario (solo admin)"""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    if is_active is not None:
        user.is_active = is_active
    if is_admin is not None:
        user.is_admin = is_admin
    
    db.commit()
    
    return {"message": "Usuario actualizado"}
