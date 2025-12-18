"""
Router de Administración
Permite al admin gestionar tokens de LucidBot de todos los usuarios
"""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel
import httpx
import json

from database import get_db, User, LucidbotConnection, LucidbotContact
from routers.auth import get_current_user
from utils import encrypt_token, decrypt_token

router = APIRouter()

# URL de LucidBot
LUCIDBOT_PHP_URL = "https://panel.lucidbot.co/php/user.php"


# ========== SCHEMAS ==========

class UserLucidbotStatus(BaseModel):
    user_id: int
    email: str
    name: Optional[str]
    has_jwt_token: bool
    page_id: Optional[str]
    token_expires: Optional[datetime]
    total_contacts: int
    total_ventas: int
    last_sync: Optional[datetime]


class SetUserTokenRequest(BaseModel):
    user_id: int
    jwt_token: str
    page_id: str


class SyncUserRequest(BaseModel):
    user_id: int


# ========== HELPERS ==========

def require_admin(current_user: User = Depends(get_current_user)):
    """Verificar que el usuario es admin"""
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Acceso denegado. Se requiere rol de administrador."
        )
    return current_user


async def validate_jwt_token(jwt_token: str, page_id: str) -> dict:
    """Validar que el JWT token funciona"""
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"token={jwt_token}; last_page_id={page_id}"
    }
    
    payload = {
        "op": "users",
        "op1": "get",
        "cdts": [],
        "oprt": 1,
        "search_text": "",
        "datatable": {
            "draw": 1,
            "start": 0,
            "length": 1,
            "orderByName": [{"column": {"name": "dt"}, "dir": "desc"}]
        },
        "pageName": "users",
        "page_id": page_id
    }
    
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                LUCIDBOT_PHP_URL,
                headers=headers,
                json=payload
            )
            
            if response.status_code != 200:
                return {"success": False, "error": f"HTTP {response.status_code}"}
            
            data = response.json()
            
            if data.get("status") != "OK":
                return {"success": False, "error": "Token inválido o expirado"}
            
            return {
                "success": True,
                "total_contacts": data.get("recordsTotal", 0)
            }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ========== ENDPOINTS ==========

@router.get("/users", response_model=List[UserLucidbotStatus])
async def get_all_users_status(
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Obtener estado de todos los usuarios con sus conexiones de LucidBot"""
    
    users = db.query(User).filter(User.is_active == True).all()
    
    result = []
    for user in users:
        # Buscar conexión de LucidBot
        connection = db.query(LucidbotConnection).filter(
            LucidbotConnection.user_id == user.id,
            LucidbotConnection.is_active == True
        ).first()
        
        # Contar contactos y ventas
        total_contacts = db.query(func.count(LucidbotContact.id)).filter(
            LucidbotContact.user_id == user.id
        ).scalar() or 0
        
        total_ventas = db.query(func.count(LucidbotContact.id)).filter(
            LucidbotContact.user_id == user.id,
            LucidbotContact.total_a_pagar > 0
        ).scalar() or 0
        
        # Última sincronización
        last_contact = db.query(LucidbotContact).filter(
            LucidbotContact.user_id == user.id
        ).order_by(LucidbotContact.synced_at.desc()).first()
        
        # Decodificar token para ver expiración
        token_expires = None
        if connection and connection.jwt_token_encrypted:
            try:
                jwt_token = decrypt_token(connection.jwt_token_encrypted)
                # Decodificar JWT para obtener expiración
                import base64
                parts = jwt_token.split('.')
                if len(parts) >= 2:
                    payload = parts[1]
                    # Agregar padding si es necesario
                    payload += '=' * (4 - len(payload) % 4)
                    decoded = json.loads(base64.b64decode(payload))
                    if "expire" in decoded:
                        token_expires = datetime.fromtimestamp(decoded["expire"])
            except:
                pass
        
        result.append(UserLucidbotStatus(
            user_id=user.id,
            email=user.email,
            name=user.name,
            has_jwt_token=bool(connection and connection.jwt_token_encrypted),
            page_id=connection.page_id if connection else None,
            token_expires=token_expires,
            total_contacts=total_contacts,
            total_ventas=total_ventas,
            last_sync=last_contact.synced_at if last_contact else None
        ))
    
    return result


@router.post("/set-token")
async def set_user_token(
    data: SetUserTokenRequest,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Configurar JWT token de LucidBot para un usuario específico"""
    
    # Verificar que el usuario existe
    user = db.query(User).filter(User.id == data.user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Usuario no encontrado"
        )
    
    # Validar token
    validation = await validate_jwt_token(data.jwt_token, data.page_id)
    if not validation.get("success"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Token inválido: {validation.get('error')}"
        )
    
    # Buscar o crear conexión
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == data.user_id
    ).first()
    
    if connection:
        connection.jwt_token_encrypted = encrypt_token(data.jwt_token)
        connection.page_id = data.page_id
        connection.is_active = True
        connection.updated_at = datetime.utcnow()
    else:
        connection = LucidbotConnection(
            user_id=data.user_id,
            jwt_token_encrypted=encrypt_token(data.jwt_token),
            page_id=data.page_id,
            is_active=True
        )
        db.add(connection)
    
    db.commit()
    
    return {
        "success": True,
        "message": f"Token configurado para {user.email}",
        "total_contacts_in_lucidbot": validation.get("total_contacts", 0)
    }


@router.post("/sync-user")
async def sync_user_contacts(
    data: SyncUserRequest,
    background_tasks: BackgroundTasks,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Iniciar sincronización de contactos para un usuario específico"""
    
    # Verificar que el usuario existe
    user = db.query(User).filter(User.id == data.user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Usuario no encontrado"
        )
    
    # Verificar que tiene token configurado
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == data.user_id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not connection or not connection.jwt_token_encrypted:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"El usuario {user.email} no tiene JWT token configurado"
        )
    
    jwt_token = decrypt_token(connection.jwt_token_encrypted)
    page_id = connection.page_id
    
    # Importar la función de sync
    from routers.sync import sync_contacts_background
    
    # Ejecutar sync en background
    background_tasks.add_task(
        sync_contacts_background,
        data.user_id,
        jwt_token,
        page_id
    )
    
    return {
        "success": True,
        "message": f"Sincronización iniciada para {user.email}",
        "status": "processing"
    }


@router.post("/sync-all")
async def sync_all_users(
    background_tasks: BackgroundTasks,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Sincronizar todos los usuarios que tienen token configurado"""
    
    # Obtener todas las conexiones activas con token
    connections = db.query(LucidbotConnection).filter(
        LucidbotConnection.is_active == True,
        LucidbotConnection.jwt_token_encrypted != None
    ).all()
    
    if not connections:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No hay usuarios con token configurado"
        )
    
    # Importar la función de sync
    from routers.sync import sync_contacts_background
    
    synced_users = []
    for conn in connections:
        user = db.query(User).filter(User.id == conn.user_id).first()
        if user:
            jwt_token = decrypt_token(conn.jwt_token_encrypted)
            background_tasks.add_task(
                sync_contacts_background,
                conn.user_id,
                jwt_token,
                conn.page_id
            )
            synced_users.append(user.email)
    
    return {
        "success": True,
        "message": f"Sincronización iniciada para {len(synced_users)} usuarios",
        "users": synced_users
    }


@router.delete("/clear-contacts/{user_id}")
async def clear_user_contacts(
    user_id: int,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Eliminar todos los contactos de un usuario (para re-sincronizar)"""
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Usuario no encontrado"
        )
    
    deleted = db.query(LucidbotContact).filter(
        LucidbotContact.user_id == user_id
    ).delete()
    
    db.commit()
    
    return {
        "success": True,
        "message": f"Eliminados {deleted} contactos de {user.email}"
    }
