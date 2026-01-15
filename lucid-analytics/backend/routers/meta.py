"""
Router de Meta Ads
Maneja OAuth y consultas a la API de Meta/Facebook Ads
"""

from fastapi import APIRouter, Depends, HTTPException, status, Request
from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import datetime
from pydantic import BaseModel
import httpx
import os

from database import get_db, User, MetaAccount
from routers.auth import get_current_user
from utils import encrypt_token, decrypt_token

router = APIRouter()

META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"


# ========== SCHEMAS ==========

class MetaAccountResponse(BaseModel):
    id: str
    name: str
    account_status: int
    currency: str
    
    
class MetaOAuthCallback(BaseModel):
    code: str


# ========== ENDPOINTS ==========

@router.get("/accounts")
async def get_meta_accounts(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener cuentas de Meta Ads conectadas"""
    
    accounts = db.query(MetaAccount).filter(
        MetaAccount.user_id == current_user.id,
        MetaAccount.is_active == True
    ).all()
    
    return {
        "accounts": [
            {
                "id": acc.account_id,
                "name": acc.account_name,
                "connected_at": acc.created_at.isoformat() if acc.created_at else None
            }
            for acc in accounts
        ]
    }


@router.post("/oauth/callback")
async def meta_oauth_callback(
    data: MetaOAuthCallback,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Callback de OAuth de Meta"""
    
    app_id = os.getenv("META_APP_ID")
    app_secret = os.getenv("META_APP_SECRET")
    redirect_uri = os.getenv("META_REDIRECT_URI", "https://lucid-analytics-frontend.vercel.app/auth/meta/callback")
    
    if not app_id or not app_secret:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Meta App credentials not configured"
        )
    
    async with httpx.AsyncClient() as client:
        # Exchange code for access token
        token_response = await client.get(
            f"{META_BASE_URL}/oauth/access_token",
            params={
                "client_id": app_id,
                "client_secret": app_secret,
                "redirect_uri": redirect_uri,
                "code": data.code
            }
        )
        
        if token_response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Error exchanging code for token"
            )
        
        token_data = token_response.json()
        access_token = token_data.get("access_token")
        
        if not access_token:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No access token received"
            )
        
        # Get ad accounts
        accounts_response = await client.get(
            f"{META_BASE_URL}/me/adaccounts",
            params={
                "access_token": access_token,
                "fields": "id,name,account_status,currency"
            }
        )
        
        if accounts_response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Error fetching ad accounts"
            )
        
        accounts_data = accounts_response.json().get("data", [])
        
        # Save accounts
        saved_accounts = []
        for account in accounts_data:
            account_id = account.get("id", "").replace("act_", "")
            
            existing = db.query(MetaAccount).filter(
                MetaAccount.user_id == current_user.id,
                MetaAccount.account_id == account_id
            ).first()
            
            if existing:
                existing.access_token_encrypted = encrypt_token(access_token)
                existing.account_name = account.get("name", "")
                existing.is_active = True
                existing.updated_at = datetime.utcnow()
            else:
                new_account = MetaAccount(
                    user_id=current_user.id,
                    account_id=account_id,
                    account_name=account.get("name", ""),
                    access_token_encrypted=encrypt_token(access_token),
                    is_active=True
                )
                db.add(new_account)
            
            saved_accounts.append({
                "id": account_id,
                "name": account.get("name", "")
            })
        
        db.commit()
        
        return {
            "message": "Meta Ads conectado exitosamente",
            "accounts": saved_accounts
        }


@router.post("/sync-accounts")
async def sync_meta_accounts(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Sincronizar cuentas de Meta Ads usando el token existente.
    Útil cuando el usuario tiene acceso a nuevos BMs o cuentas publicitarias
    sin necesidad de re-hacer OAuth completo.
    """
    
    # Buscar una cuenta activa con token válido
    existing_account = db.query(MetaAccount).filter(
        MetaAccount.user_id == current_user.id,
        MetaAccount.is_active == True,
        MetaAccount.access_token_encrypted.isnot(None)
    ).first()
    
    if not existing_account:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No hay cuentas de Meta conectadas. Primero conecta Meta Ads."
        )
    
    # Desencriptar token
    try:
        access_token = decrypt_token(existing_account.access_token_encrypted)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Token de Meta inválido. Por favor reconecta Meta Ads."
        )
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        # Verificar que el token sigue siendo válido
        debug_response = await client.get(
            f"{META_BASE_URL}/debug_token",
            params={
                "input_token": access_token,
                "access_token": access_token
            }
        )
        
        if debug_response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Token de Meta expirado. Por favor reconecta Meta Ads."
            )
        
        debug_data = debug_response.json().get("data", {})
        if not debug_data.get("is_valid", False):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Token de Meta inválido o expirado. Por favor reconecta Meta Ads."
            )
        
        # Obtener todas las cuentas publicitarias actuales
        accounts_response = await client.get(
            f"{META_BASE_URL}/me/adaccounts",
            params={
                "access_token": access_token,
                "fields": "id,name,account_status,currency,business{id,name}",
                "limit": 500
            }
        )
        
        if accounts_response.status_code != 200:
            error_data = accounts_response.json()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_data.get("error", {}).get("message", "Error obteniendo cuentas de Meta")
            )
        
        accounts_data = accounts_response.json().get("data", [])
        
        # Obtener IDs de cuentas existentes
        existing_account_ids = set(
            acc.account_id for acc in db.query(MetaAccount).filter(
                MetaAccount.user_id == current_user.id
            ).all()
        )
        
        new_accounts = []
        updated_accounts = []
        
        for account in accounts_data:
            account_id = account.get("id", "").replace("act_", "")
            account_name = account.get("name", "")
            
            # Agregar info del Business Manager al nombre si está disponible
            business = account.get("business", {})
            business_name = business.get("name", "")
            
            # Si tiene BM, agregar prefijo para identificarlo
            if business_name:
                display_name = f"[{business_name}] {account_name}"
            else:
                display_name = account_name
            
            existing = db.query(MetaAccount).filter(
                MetaAccount.user_id == current_user.id,
                MetaAccount.account_id == account_id
            ).first()
            
            if existing:
                # Actualizar cuenta existente
                existing.access_token_encrypted = encrypt_token(access_token)
                existing.account_name = display_name
                existing.is_active = True
                existing.updated_at = datetime.utcnow()
                updated_accounts.append({
                    "id": account_id,
                    "name": display_name,
                    "business": business_name
                })
            else:
                # Crear nueva cuenta
                new_account = MetaAccount(
                    user_id=current_user.id,
                    account_id=account_id,
                    account_name=display_name,
                    access_token_encrypted=encrypt_token(access_token),
                    is_active=True
                )
                db.add(new_account)
                new_accounts.append({
                    "id": account_id,
                    "name": display_name,
                    "business": business_name
                })
        
        db.commit()
        
        return {
            "message": f"Sincronización completada. {len(new_accounts)} nuevas, {len(updated_accounts)} actualizadas.",
            "new_accounts": new_accounts,
            "updated_accounts": updated_accounts,
            "total_accounts": len(accounts_data)
        }


@router.delete("/disconnect/{account_id}")
async def disconnect_meta_account(
    account_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Desconectar una cuenta de Meta Ads"""
    
    account = db.query(MetaAccount).filter(
        MetaAccount.user_id == current_user.id,
        MetaAccount.account_id == account_id
    ).first()
    
    if not account:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cuenta no encontrada"
        )
    
    account.is_active = False
    db.commit()
    
    return {"message": "Cuenta desconectada"}


@router.get("/insights/{account_id}")
async def get_account_insights(
    account_id: str,
    start_date: str,
    end_date: str,
    level: str = "ad",
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener insights de una cuenta de Meta Ads"""
    
    account = db.query(MetaAccount).filter(
        MetaAccount.user_id == current_user.id,
        MetaAccount.account_id == account_id,
        MetaAccount.is_active == True
    ).first()
    
    if not account:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cuenta no encontrada"
        )
    
    access_token = decrypt_token(account.access_token_encrypted)
    
    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.get(
            f"{META_BASE_URL}/act_{account_id}/insights",
            params={
                "access_token": access_token,
                "level": level,
                "fields": "ad_id,ad_name,campaign_id,campaign_name,adset_id,adset_name,spend,impressions,clicks,ctr,cpm,cpc,reach,actions,cost_per_action_type",
                "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
                "limit": 500
            }
        )
        
        if response.status_code != 200:
            error_data = response.json()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=error_data.get("error", {}).get("message", "Error de Meta API")
            )
        
        return response.json()
