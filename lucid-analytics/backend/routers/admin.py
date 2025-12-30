"""
Router de Administración
Permite al admin gestionar sincronización de LucidBot Y Dropi
"""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel
import httpx
import json

from database import (
    get_db, User, 
    LucidbotConnection, LucidbotContact,
    DropiConnection, DropiOrder, DropiWalletHistory
)
from routers.auth import get_current_user
from utils import encrypt_token, decrypt_token

router = APIRouter()

# URL de LucidBot
LUCIDBOT_PHP_URL = "https://panel.lucidbot.co/php/user.php"


# ========== SCHEMAS ==========

class UserSyncStatus(BaseModel):
    user_id: int
    email: str
    name: Optional[str]
    # LucidBot
    has_lucidbot_token: bool
    lucidbot_page_id: Optional[str]
    lucidbot_contacts: int
    lucidbot_ventas: int
    lucidbot_last_sync: Optional[datetime]
    # Dropi
    has_dropi_connection: bool
    dropi_country: Optional[str]
    dropi_orders: int
    dropi_wallet_movements: int
    dropi_sync_status: Optional[str]
    dropi_last_sync: Optional[datetime]


class SetUserTokenRequest(BaseModel):
    user_id: int
    jwt_token: str
    page_id: str


class SyncUserRequest(BaseModel):
    user_id: int


class SyncDropiRequest(BaseModel):
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
    """Validar que el JWT token de LucidBot funciona"""
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

@router.get("/users", response_model=List[UserSyncStatus])
async def get_all_users_status(
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Obtener estado de todos los usuarios con sus conexiones"""
    
    users = db.query(User).filter(User.is_active == True).all()
    
    result = []
    for user in users:
        # ========== LUCIDBOT ==========
        lucidbot_conn = db.query(LucidbotConnection).filter(
            LucidbotConnection.user_id == user.id,
            LucidbotConnection.is_active == True
        ).first()
        
        lucidbot_contacts = db.query(func.count(LucidbotContact.id)).filter(
            LucidbotContact.user_id == user.id
        ).scalar() or 0
        
        lucidbot_ventas = db.query(func.count(LucidbotContact.id)).filter(
            LucidbotContact.user_id == user.id,
            LucidbotContact.total_a_pagar > 0
        ).scalar() or 0
        
        last_lucidbot_contact = db.query(LucidbotContact).filter(
            LucidbotContact.user_id == user.id
        ).order_by(LucidbotContact.synced_at.desc()).first()
        
        # ========== DROPI ==========
        dropi_conn = db.query(DropiConnection).filter(
            DropiConnection.user_id == user.id,
            DropiConnection.is_active == True
        ).first()
        
        dropi_orders = db.query(func.count(DropiOrder.id)).filter(
            DropiOrder.user_id == user.id
        ).scalar() or 0
        
        dropi_wallet = db.query(func.count(DropiWalletHistory.id)).filter(
            DropiWalletHistory.user_id == user.id
        ).scalar() or 0
        
        result.append(UserSyncStatus(
            user_id=user.id,
            email=user.email,
            name=user.name,
            # LucidBot
            has_lucidbot_token=bool(lucidbot_conn and lucidbot_conn.jwt_token_encrypted),
            lucidbot_page_id=lucidbot_conn.page_id if lucidbot_conn else None,
            lucidbot_contacts=lucidbot_contacts,
            lucidbot_ventas=lucidbot_ventas,
            lucidbot_last_sync=last_lucidbot_contact.synced_at if last_lucidbot_contact else None,
            # Dropi
            has_dropi_connection=bool(dropi_conn),
            dropi_country=dropi_conn.country if dropi_conn else None,
            dropi_orders=dropi_orders,
            dropi_wallet_movements=dropi_wallet,
            dropi_sync_status=dropi_conn.sync_status if dropi_conn else None,
            dropi_last_sync=dropi_conn.last_orders_sync if dropi_conn else None
        ))
    
    return result


# ========== DEBUG ENDPOINTS ==========

@router.get("/debug/ad-ids/{user_id}")
async def debug_user_ad_ids(
    user_id: int,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    DEBUG: Ver qué ad_ids existen en la BD para un usuario.
    Esto ayuda a diagnosticar por qué los contactos no se conectan con Meta Ads.
    """
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    # Obtener ad_ids únicos con conteo
    ad_id_counts = db.query(
        LucidbotContact.ad_id,
        func.count(LucidbotContact.id).label('count')
    ).filter(
        LucidbotContact.user_id == user_id
    ).group_by(
        LucidbotContact.ad_id
    ).order_by(
        func.count(LucidbotContact.id).desc()
    ).limit(50).all()
    
    # Total de contactos
    total = db.query(func.count(LucidbotContact.id)).filter(
        LucidbotContact.user_id == user_id
    ).scalar() or 0
    
    # Contactos con ad_id null o vacío
    null_count = db.query(func.count(LucidbotContact.id)).filter(
        LucidbotContact.user_id == user_id,
        (LucidbotContact.ad_id == None) | (LucidbotContact.ad_id == "")
    ).scalar() or 0
    
    # Muestra de contactos recientes
    recent_contacts = db.query(LucidbotContact).filter(
        LucidbotContact.user_id == user_id
    ).order_by(LucidbotContact.contact_created_at.desc()).limit(10).all()
    
    return {
        "user_id": user_id,
        "email": user.email,
        "total_contacts": total,
        "contacts_without_ad_id": null_count,
        "contacts_with_ad_id": total - null_count,
        "unique_ad_ids": [
            {"ad_id": ad_id or "(null/empty)", "count": count}
            for ad_id, count in ad_id_counts
        ],
        "recent_contacts_sample": [
            {
                "id": c.lucidbot_id,
                "name": c.full_name,
                "ad_id": c.ad_id,
                "created": c.contact_created_at.isoformat() if c.contact_created_at else None,
                "total_a_pagar": c.total_a_pagar
            }
            for c in recent_contacts
        ]
    }


@router.get("/debug/sample-contacts/{user_id}")
async def debug_sample_contacts(
    user_id: int,
    limit: int = 20,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    DEBUG: Ver muestra de contactos con todos sus campos.
    """
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    contacts = db.query(LucidbotContact).filter(
        LucidbotContact.user_id == user_id
    ).order_by(LucidbotContact.contact_created_at.desc()).limit(limit).all()
    
    return {
        "user_id": user_id,
        "email": user.email,
        "sample_size": len(contacts),
        "contacts": [
            {
                "lucidbot_id": c.lucidbot_id,
                "full_name": c.full_name,
                "phone": c.phone,
                "ad_id": c.ad_id,
                "total_a_pagar": c.total_a_pagar,
                "producto": c.producto,
                "calificacion": c.calificacion,
                "contact_created_at": c.contact_created_at.isoformat() if c.contact_created_at else None,
                "synced_at": c.synced_at.isoformat() if c.synced_at else None
            }
            for c in contacts
        ]
    }


@router.get("/debug/lucidbot-raw/{user_id}")
async def debug_lucidbot_raw(
    user_id: int,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    DEBUG: Ver datos RAW de LucidBot para un usuario.
    Esto muestra exactamente qué devuelve la API de LucidBot, incluyendo todos los campos.
    """
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == user_id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not connection or not connection.jwt_token_encrypted:
        return {"error": "No hay conexión de LucidBot activa", "user_id": user_id}
    
    jwt_token = decrypt_token(connection.jwt_token_encrypted)
    page_id = connection.page_id
    
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
            "length": 5,
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
                return {"error": f"HTTP {response.status_code}", "user_id": user_id}
            
            data = response.json()
            
            return {
                "user_id": user_id,
                "email": user.email,
                "page_id": page_id,
                "lucidbot_status": data.get("status"),
                "total_records": data.get("recordsTotal"),
                "raw_contacts": data.get("data", [])[:5],
                "all_fields_first_contact": data.get("data", [{}])[0] if data.get("data") else {}
            }
    except Exception as e:
        return {"error": str(e), "user_id": user_id}


# ========== DROPI DEBUG ENDPOINTS ==========

@router.get("/debug/dropi-connection/{user_id}")
async def debug_dropi_connection(
    user_id: int,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    DEBUG: Ver estado de conexión de Dropi para un usuario.
    Muestra si hay credenciales guardadas (censuradas) y estado de sync.
    """
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == user_id
    ).first()
    
    if not connection:
        return {
            "user_id": user_id,
            "email": user.email,
            "has_connection": False,
            "message": "No hay conexión de Dropi configurada"
        }
    
    # Mostrar credenciales censuradas
    has_email = bool(connection.email_encrypted)
    has_password = bool(connection.password_encrypted)
    
    email_preview = None
    if has_email:
        try:
            email = decrypt_token(connection.email_encrypted)
            email_preview = email[:3] + "***" + email[email.find("@"):] if "@" in email else email[:3] + "***"
        except:
            email_preview = "(error al descifrar)"
    
    return {
        "user_id": user_id,
        "email": user.email,
        "has_connection": True,
        "is_active": connection.is_active,
        "country": connection.country,
        "dropi_user_id": connection.dropi_user_id,
        "has_email_encrypted": has_email,
        "has_password_encrypted": has_password,
        "email_preview": email_preview,
        "sync_status": connection.sync_status,
        "last_orders_sync": connection.last_orders_sync.isoformat() if connection.last_orders_sync else None,
        "last_wallet_sync": connection.last_wallet_sync.isoformat() if connection.last_wallet_sync else None,
        "created_at": connection.created_at.isoformat() if connection.created_at else None
    }


@router.post("/debug/dropi-test-login/{user_id}")
async def debug_dropi_test_login(
    user_id: int,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """
    DEBUG: Probar login de Dropi para un usuario.
    Esto ayuda a diagnosticar por qué falla la sincronización.
    """
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == user_id,
        DropiConnection.is_active == True
    ).first()
    
    if not connection:
        return {
            "success": False,
            "error": "No hay conexión de Dropi activa",
            "user_id": user_id,
            "email": user.email
        }
    
    if not connection.email_encrypted or not connection.password_encrypted:
        return {
            "success": False,
            "error": "Faltan credenciales (email o password)",
            "has_email": bool(connection.email_encrypted),
            "has_password": bool(connection.password_encrypted)
        }
    
    try:
        from routers.sync_dropi import dropi_login
        
        email = decrypt_token(connection.email_encrypted)
        password = decrypt_token(connection.password_encrypted)
        
        # Censurar para el log
        email_censored = email[:3] + "***" + email[email.find("@"):] if "@" in email else email[:3] + "***"
        
        result = await dropi_login(email, password, connection.country)
        
        if result.get("success"):
            return {
                "success": True,
                "message": "Login exitoso",
                "user_id": user_id,
                "dropi_email": email_censored,
                "country": connection.country,
                "dropi_user_id": result.get("user_id"),
                "token_preview": result.get("token", "")[:20] + "..." if result.get("token") else None
            }
        else:
            return {
                "success": False,
                "error": result.get("error", "Login failed"),
                "user_id": user_id,
                "dropi_email": email_censored,
                "country": connection.country
            }
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "user_id": user_id
        }


# ========== LUCIDBOT ENDPOINTS ==========

@router.post("/lucidbot/set-token")
async def set_lucidbot_token(
    data: SetUserTokenRequest,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Configurar JWT token de LucidBot para un usuario"""
    
    user = db.query(User).filter(User.id == data.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    # Validar token
    validation = await validate_jwt_token(data.jwt_token, data.page_id)
    if not validation.get("success"):
        raise HTTPException(
            status_code=400,
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


@router.post("/lucidbot/sync-user")
async def sync_lucidbot_user(
    data: SyncUserRequest,
    background_tasks: BackgroundTasks,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Sincronizar LucidBot para un usuario"""
    
    user = db.query(User).filter(User.id == data.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == data.user_id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not connection or not connection.jwt_token_encrypted:
        raise HTTPException(status_code=400, detail=f"El usuario {user.email} no tiene token configurado")
    
    jwt_token = decrypt_token(connection.jwt_token_encrypted)
    page_id = connection.page_id
    
    from routers.sync import sync_contacts_background
    background_tasks.add_task(sync_contacts_background, data.user_id, jwt_token, page_id)
    
    return {
        "success": True,
        "message": f"Sincronización LucidBot iniciada para {user.email}"
    }


@router.post("/lucidbot/sync-all")
async def sync_all_lucidbot(
    background_tasks: BackgroundTasks,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Sincronizar LucidBot para todos los usuarios con token"""
    
    connections = db.query(LucidbotConnection).filter(
        LucidbotConnection.is_active == True,
        LucidbotConnection.jwt_token_encrypted != None
    ).all()
    
    if not connections:
        raise HTTPException(status_code=400, detail="No hay usuarios con token configurado")
    
    from routers.sync import sync_contacts_background
    
    synced_users = []
    for conn in connections:
        user = db.query(User).filter(User.id == conn.user_id).first()
        if user:
            jwt_token = decrypt_token(conn.jwt_token_encrypted)
            background_tasks.add_task(sync_contacts_background, conn.user_id, jwt_token, conn.page_id)
            synced_users.append(user.email)
    
    return {
        "success": True,
        "message": f"Sincronización iniciada para {len(synced_users)} usuarios",
        "users": synced_users
    }


@router.delete("/lucidbot/clear-contacts/{user_id}")
async def clear_lucidbot_contacts(
    user_id: int,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Eliminar contactos de LucidBot de un usuario"""
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    deleted = db.query(LucidbotContact).filter(
        LucidbotContact.user_id == user_id
    ).delete()
    
    db.commit()
    
    return {
        "success": True,
        "message": f"Eliminados {deleted} contactos de {user.email}"
    }


# ========== DROPI ENDPOINTS ==========

@router.post("/dropi/sync-user")
async def sync_dropi_user(
    data: SyncDropiRequest,
    background_tasks: BackgroundTasks,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Sincronizar Dropi para un usuario"""
    
    user = db.query(User).filter(User.id == data.user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == data.user_id,
        DropiConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=400, detail=f"El usuario {user.email} no tiene Dropi conectado")
    
    from routers.sync_dropi import sync_dropi_background
    background_tasks.add_task(sync_dropi_background, data.user_id)
    
    # Marcar como sincronizando
    connection.sync_status = "syncing"
    db.commit()
    
    return {
        "success": True,
        "message": f"Sincronización Dropi iniciada para {user.email}"
    }


@router.post("/dropi/sync-all")
async def sync_all_dropi(
    background_tasks: BackgroundTasks,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Sincronizar Dropi para todos los usuarios conectados"""
    
    connections = db.query(DropiConnection).filter(
        DropiConnection.is_active == True
    ).all()
    
    if not connections:
        raise HTTPException(status_code=400, detail="No hay usuarios con Dropi conectado")
    
    from routers.sync_dropi import sync_dropi_background
    
    synced_users = []
    for conn in connections:
        user = db.query(User).filter(User.id == conn.user_id).first()
        if user:
            background_tasks.add_task(sync_dropi_background, conn.user_id)
            conn.sync_status = "syncing"
            synced_users.append(user.email)
    
    db.commit()
    
    return {
        "success": True,
        "message": f"Sincronización Dropi iniciada para {len(synced_users)} usuarios",
        "users": synced_users
    }


@router.delete("/dropi/clear-data/{user_id}")
async def clear_dropi_data(
    user_id: int,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Eliminar datos de Dropi de un usuario (para re-sincronizar)"""
    
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="Usuario no encontrado")
    
    deleted_orders = db.query(DropiOrder).filter(
        DropiOrder.user_id == user_id
    ).delete()
    
    deleted_wallet = db.query(DropiWalletHistory).filter(
        DropiWalletHistory.user_id == user_id
    ).delete()
    
    # Reset sync status
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == user_id
    ).first()
    if connection:
        connection.last_orders_sync = None
        connection.last_wallet_sync = None
        connection.sync_status = "pending"
    
    db.commit()
    
    return {
        "success": True,
        "message": f"Eliminados {deleted_orders} órdenes y {deleted_wallet} movimientos de wallet de {user.email}"
    }


@router.get("/dropi/sync-status/{user_id}")
async def get_dropi_sync_status(
    user_id: int,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Obtener estado de sincronización de Dropi para un usuario"""
    
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == user_id
    ).first()
    
    if not connection:
        return {"connected": False}
    
    orders_count = db.query(func.count(DropiOrder.id)).filter(
        DropiOrder.user_id == user_id
    ).scalar() or 0
    
    wallet_count = db.query(func.count(DropiWalletHistory.id)).filter(
        DropiWalletHistory.user_id == user_id
    ).scalar() or 0
    
    # Stats de órdenes
    delivered = db.query(func.count(DropiOrder.id)).filter(
        DropiOrder.user_id == user_id,
        DropiOrder.status == "ENTREGADO"
    ).scalar() or 0
    
    returned = db.query(func.count(DropiOrder.id)).filter(
        DropiOrder.user_id == user_id,
        DropiOrder.status == "DEVOLUCION"
    ).scalar() or 0
    
    paid = db.query(func.count(DropiOrder.id)).filter(
        DropiOrder.user_id == user_id,
        DropiOrder.is_paid == True
    ).scalar() or 0
    
    return {
        "connected": True,
        "sync_status": connection.sync_status,
        "last_orders_sync": connection.last_orders_sync.isoformat() if connection.last_orders_sync else None,
        "last_wallet_sync": connection.last_wallet_sync.isoformat() if connection.last_wallet_sync else None,
        "total_orders": orders_count,
        "total_wallet_movements": wallet_count,
        "delivered": delivered,
        "returned": returned,
        "paid_orders": paid
    }


# ========== SYNC ALL (LUCIDBOT + DROPI) ==========

@router.post("/sync-all")
async def sync_all_platforms(
    background_tasks: BackgroundTasks,
    admin: User = Depends(require_admin),
    db: Session = Depends(get_db)
):
    """Sincronizar LucidBot Y Dropi para todos los usuarios"""
    
    results = {"lucidbot": [], "dropi": []}
    
    # LucidBot
    lucidbot_conns = db.query(LucidbotConnection).filter(
        LucidbotConnection.is_active == True,
        LucidbotConnection.jwt_token_encrypted != None
    ).all()
    
    from routers.sync import sync_contacts_background
    
    for conn in lucidbot_conns:
        user = db.query(User).filter(User.id == conn.user_id).first()
        if user:
            jwt_token = decrypt_token(conn.jwt_token_encrypted)
            background_tasks.add_task(sync_contacts_background, conn.user_id, jwt_token, conn.page_id)
            results["lucidbot"].append(user.email)
    
    # Dropi
    dropi_conns = db.query(DropiConnection).filter(
        DropiConnection.is_active == True
    ).all()
    
    from routers.sync_dropi import sync_dropi_background
    
    for conn in dropi_conns:
        user = db.query(User).filter(User.id == conn.user_id).first()
        if user:
            background_tasks.add_task(sync_dropi_background, conn.user_id)
            conn.sync_status = "syncing"
            results["dropi"].append(user.email)
    
    db.commit()
    
    return {
        "success": True,
        "message": f"Sincronización iniciada",
        "lucidbot_users": len(results["lucidbot"]),
        "dropi_users": len(results["dropi"]),
        "details": results
    }
