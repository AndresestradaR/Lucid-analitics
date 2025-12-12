"""
Router de LucidBot
Maneja conexión con la API de LucidBot para tracking de leads y ventas
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime
from pydantic import BaseModel
import httpx

from database import get_db, User, LucidbotConnection
from routers.auth import get_current_user
from utils import encrypt_token, decrypt_token

router = APIRouter()

LUCIDBOT_BASE_URL = "https://panel.lucidbot.co/api"


# ========== SCHEMAS ==========

class LucidbotConnectRequest(BaseModel):
    api_token: str


class LucidbotConnectionResponse(BaseModel):
    id: int
    account_id: Optional[str]
    is_active: bool
    connected: bool
    created_at: datetime
    
    class Config:
        from_attributes = True


# ========== HELPERS ==========

async def verify_lucidbot_token(api_token: str) -> dict:
    """Verifica token de LucidBot haciendo una petición de prueba"""
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            # Probar obteniendo info de la cuenta
            response = await client.get(
                f"{LUCIDBOT_BASE_URL}/account",
                headers={
                    "X-ACCESS-TOKEN": api_token,
                    "Accept": "application/json"
                }
            )
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "valid": True,
                    "account_id": str(data.get("id", ""))
                }
            elif response.status_code == 401:
                return {"valid": False, "error": "Token inválido o expirado"}
            else:
                return {"valid": False, "error": f"Error de LucidBot: {response.status_code}"}
                
        except httpx.TimeoutException:
            return {"valid": False, "error": "Timeout conectando con LucidBot"}
        except Exception as e:
            return {"valid": False, "error": str(e)}


# ========== ENDPOINTS ==========

@router.post("/connect")
async def connect_lucidbot(
    data: LucidbotConnectRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Conectar cuenta de LucidBot usando API token"""
    
    # Verificar token
    verification = await verify_lucidbot_token(data.api_token)
    
    if not verification.get("valid"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Token inválido: {verification.get('error', 'Error desconocido')}"
        )
    
    # Buscar conexión existente
    existing = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id
    ).first()
    
    if existing:
        # Actualizar
        existing.api_token_encrypted = encrypt_token(data.api_token)
        existing.account_id = verification.get("account_id")
        existing.is_active = True
        existing.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(existing)
        return {
            "message": "LucidBot actualizado exitosamente",
            "account_id": existing.account_id
        }
    
    # Crear nueva
    connection = LucidbotConnection(
        user_id=current_user.id,
        api_token_encrypted=encrypt_token(data.api_token),
        account_id=verification.get("account_id"),
        is_active=True
    )
    db.add(connection)
    db.commit()
    db.refresh(connection)
    
    return {
        "message": "LucidBot conectado exitosamente",
        "account_id": connection.account_id
    }


@router.get("/status")
async def get_lucidbot_status(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Estado de conexión de LucidBot"""
    
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id
    ).first()
    
    if not connection or not connection.is_active:
        return {
            "connected": False,
            "message": "No hay conexión activa de LucidBot"
        }
    
    return {
        "connected": True,
        "account_id": connection.account_id,
        "created_at": connection.created_at.isoformat() if connection.created_at else None
    }


@router.delete("/disconnect")
async def disconnect_lucidbot(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Desconectar cuenta de LucidBot"""
    
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No hay conexión de LucidBot para desconectar"
        )
    
    db.delete(connection)
    db.commit()
    
    return {"message": "LucidBot desconectado exitosamente"}


@router.get("/contacts")
async def get_contacts(
    ad_id: Optional[str] = None,
    limit: int = 100,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener contactos de LucidBot, opcionalmente filtrados por ad_id"""
    
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No hay conexión activa de LucidBot"
        )
    
    api_token = decrypt_token(connection.api_token_encrypted)
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            if ad_id:
                # Buscar por ad_id usando custom field
                response = await client.get(
                    f"{LUCIDBOT_BASE_URL}/users/find_by_custom_field",
                    headers={
                        "X-ACCESS-TOKEN": api_token,
                        "Accept": "application/json"
                    },
                    params={
                        "field_id": "728462",  # Campo de Ad ID en LucidBot
                        "value": ad_id
                    }
                )
            else:
                # Obtener todos los contactos
                response = await client.get(
                    f"{LUCIDBOT_BASE_URL}/users",
                    headers={
                        "X-ACCESS-TOKEN": api_token,
                        "Accept": "application/json"
                    },
                    params={"limit": limit}
                )
            
            if response.status_code != 200:
                return {"contacts": [], "error": f"Error de LucidBot: {response.status_code}"}
            
            contacts = response.json().get("data", [])
            
            # Procesar contactos
            processed = []
            total_leads = 0
            total_sales = 0
            total_revenue = 0
            
            for contact in contacts:
                custom_fields = contact.get("custom_fields", {})
                total_paid = custom_fields.get("Total a pagar")
                
                is_sale = False
                amount = 0
                
                if total_paid:
                    try:
                        amount = float(total_paid)
                        is_sale = True
                        total_sales += 1
                        total_revenue += amount
                    except ValueError:
                        total_leads += 1
                else:
                    total_leads += 1
                
                processed.append({
                    "id": contact.get("id"),
                    "name": contact.get("full_name", ""),
                    "phone": contact.get("phone", ""),
                    "created_at": contact.get("created_at", ""),
                    "is_sale": is_sale,
                    "amount": amount,
                    "calificacion": custom_fields.get("Calificacion_LucidSales", ""),
                    "producto": custom_fields.get("Producto_Ordenados", "")
                })
            
            return {
                "contacts": processed,
                "summary": {
                    "total_contacts": len(processed),
                    "leads": total_leads,
                    "sales": total_sales,
                    "revenue": total_revenue
                }
            }
            
        except httpx.TimeoutException:
            raise HTTPException(
                status_code=status.HTTP_504_GATEWAY_TIMEOUT,
                detail="Timeout conectando con LucidBot"
            )
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error: {str(e)}"
            )
