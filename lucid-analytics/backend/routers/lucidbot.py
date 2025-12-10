"""
Router de LucidBot
Maneja conexión y consultas a la API de LucidBot
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
import httpx

from database import get_db, User, LucidbotConnection, Sale
from schemas import LucidbotConnectRequest, LucidbotConnectionResponse
from routers.auth import get_current_user
from utils import encrypt_token, decrypt_token

router = APIRouter()

LUCIDBOT_BASE_URL = "https://panel.lucidbot.co/api"


# ========== CONEXIÓN ==========

@router.post("/connect", response_model=LucidbotConnectionResponse)
async def connect_lucidbot(
    data: LucidbotConnectRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Conectar cuenta de LucidBot"""
    
    # Verificar que el token funciona
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{LUCIDBOT_BASE_URL}/accounts/me",
            headers={
                "X-ACCESS-TOKEN": data.api_token,
                "Accept": "application/json"
            }
        )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Token de LucidBot inválido"
            )
        
        account_data = response.json()
        account_id = str(account_data.get("id", ""))
    
    # Buscar conexión existente
    existing = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id
    ).first()
    
    if existing:
        # Actualizar
        existing.api_token_encrypted = encrypt_token(data.api_token)
        existing.account_id = account_id
        existing.is_active = True
        db.commit()
        db.refresh(existing)
        return LucidbotConnectionResponse.model_validate(existing)
    
    # Crear nueva
    connection = LucidbotConnection(
        user_id=current_user.id,
        api_token_encrypted=encrypt_token(data.api_token),
        account_id=account_id
    )
    db.add(connection)
    db.commit()
    db.refresh(connection)
    
    return LucidbotConnectionResponse.model_validate(connection)


@router.get("/connection", response_model=LucidbotConnectionResponse)
async def get_connection(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener estado de conexión de LucidBot"""
    
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No hay conexión de LucidBot"
        )
    
    return LucidbotConnectionResponse.model_validate(connection)


@router.delete("/disconnect")
async def disconnect_lucidbot(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Desconectar LucidBot"""
    
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id
    ).first()
    
    if connection:
        connection.is_active = False
        db.commit()
    
    return {"message": "LucidBot desconectado"}


# ========== CONSULTAS ==========

@router.get("/contacts/by-ad/{ad_id}")
async def get_contacts_by_ad(
    ad_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener contactos de LucidBot por Ad ID"""
    
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No hay conexión de LucidBot"
        )
    
    api_token = decrypt_token(connection.api_token_encrypted)
    
    async with httpx.AsyncClient() as client:
        # Campo "Anuncio Facebook" tiene ID 728462 (según pruebas previas)
        # Pero puede variar por cuenta, así que primero obtenemos los custom fields
        
        response = await client.get(
            f"{LUCIDBOT_BASE_URL}/contacts/find_by_custom_field",
            headers={
                "X-ACCESS-TOKEN": api_token,
                "Accept": "application/json"
            },
            params={
                "field_id": "728462",  # ID del campo "Anuncio Facebook"
                "value": ad_id
            }
        )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Error al consultar LucidBot: {response.text}"
            )
        
        data = response.json()
        contacts = data.get("data", [])
        
        # Procesar contactos
        result = {
            "ad_id": ad_id,
            "total_contacts": len(contacts),
            "leads": [],
            "sales": [],
            "total_revenue": 0
        }
        
        for contact in contacts:
            custom_fields = contact.get("custom_fields", {})
            
            contact_info = {
                "id": contact.get("id"),
                "name": contact.get("full_name"),
                "phone": contact.get("phone"),
                "created_at": contact.get("created_at")
            }
            
            # Verificar si es venta (tiene "Total a pagar")
            total_paid = custom_fields.get("Total a pagar")
            
            if total_paid:
                try:
                    amount = float(total_paid)
                    contact_info["amount"] = amount
                    contact_info["product"] = custom_fields.get("Producto_Ordenados", "")
                    result["sales"].append(contact_info)
                    result["total_revenue"] += amount
                except ValueError:
                    result["leads"].append(contact_info)
            else:
                result["leads"].append(contact_info)
        
        result["total_leads"] = len(result["leads"])
        result["total_sales"] = len(result["sales"])
        
        return result


@router.get("/custom-fields")
async def get_custom_fields(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener custom fields de LucidBot (para configuración)"""
    
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No hay conexión de LucidBot"
        )
    
    api_token = decrypt_token(connection.api_token_encrypted)
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{LUCIDBOT_BASE_URL}/accounts/custom_fields",
            headers={
                "X-ACCESS-TOKEN": api_token,
                "Accept": "application/json"
            }
        )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Error al obtener custom fields"
            )
        
        return response.json()


@router.get("/all-ad-ids")
async def get_all_ad_ids(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Obtener todos los Ad IDs únicos de LucidBot
    Útil para saber qué anuncios tienen leads/ventas
    """
    
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No hay conexión de LucidBot"
        )
    
    api_token = decrypt_token(connection.api_token_encrypted)
    
    # Obtener contactos recientes con paginación
    all_ad_ids = set()
    offset = 0
    limit = 100
    
    async with httpx.AsyncClient() as client:
        while True:
            response = await client.get(
                f"{LUCIDBOT_BASE_URL}/contacts",
                headers={
                    "X-ACCESS-TOKEN": api_token,
                    "Accept": "application/json"
                },
                params={
                    "offset": offset,
                    "limit": limit
                },
                timeout=30
            )
            
            if response.status_code != 200:
                break
            
            data = response.json()
            contacts = data.get("data", [])
            
            if not contacts:
                break
            
            for contact in contacts:
                custom_fields = contact.get("custom_fields", {})
                ad_id = custom_fields.get("Anuncio Facebook")
                if ad_id:
                    all_ad_ids.add(ad_id)
            
            offset += limit
            
            # Límite de seguridad
            if offset > 10000:
                break
    
    return {
        "ad_ids": list(all_ad_ids),
        "count": len(all_ad_ids)
    }
