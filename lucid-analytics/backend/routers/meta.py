"""
Router de Meta Ads
Maneja OAuth y consultas a la API de Meta
"""

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
import httpx
import os

from database import get_db, User, MetaAccount
from schemas import MetaAccountResponse, MetaAdMetrics
from routers.auth import get_current_user
from utils import encrypt_token, decrypt_token

router = APIRouter()

# Configuración de Meta
META_APP_ID = os.getenv("META_APP_ID", "")
META_APP_SECRET = os.getenv("META_APP_SECRET", "")
META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"


# ========== OAUTH ==========

@router.get("/auth-url")
async def get_auth_url(
    redirect_uri: str,
    current_user: User = Depends(get_current_user)
):
    """Obtener URL para iniciar OAuth de Meta"""
    
    if not META_APP_ID:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="META_APP_ID no configurado"
        )
    
    # Permisos necesarios para Ads
    scopes = [
        "ads_read",
        "ads_management",
        "business_management",
        "read_insights"
    ]
    
    auth_url = (
        f"https://www.facebook.com/{META_API_VERSION}/dialog/oauth?"
        f"client_id={META_APP_ID}"
        f"&redirect_uri={redirect_uri}"
        f"&scope={','.join(scopes)}"
        f"&state={current_user.id}"  # Para identificar al usuario en callback
    )
    
    return {"auth_url": auth_url}


@router.post("/callback")
async def oauth_callback(
    code: str,
    redirect_uri: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Procesar callback de OAuth de Meta"""
    
    if not META_APP_ID or not META_APP_SECRET:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Credenciales de Meta no configuradas"
        )
    
    async with httpx.AsyncClient() as client:
        # Intercambiar código por access token
        token_response = await client.get(
            f"{META_BASE_URL}/oauth/access_token",
            params={
                "client_id": META_APP_ID,
                "client_secret": META_APP_SECRET,
                "redirect_uri": redirect_uri,
                "code": code
            }
        )
        
        if token_response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Error al obtener token: {token_response.text}"
            )
        
        token_data = token_response.json()
        access_token = token_data.get("access_token")
        
        # Obtener token de larga duración
        long_token_response = await client.get(
            f"{META_BASE_URL}/oauth/access_token",
            params={
                "grant_type": "fb_exchange_token",
                "client_id": META_APP_ID,
                "client_secret": META_APP_SECRET,
                "fb_exchange_token": access_token
            }
        )
        
        if long_token_response.status_code == 200:
            long_token_data = long_token_response.json()
            access_token = long_token_data.get("access_token", access_token)
            expires_in = long_token_data.get("expires_in", 5184000)  # 60 días default
        else:
            expires_in = 3600
        
        # Obtener información del usuario de Meta
        me_response = await client.get(
            f"{META_BASE_URL}/me",
            params={"access_token": access_token}
        )
        
        meta_user_id = None
        if me_response.status_code == 200:
            meta_user_id = me_response.json().get("id")
        
        # Obtener Ad Accounts
        ad_accounts_response = await client.get(
            f"{META_BASE_URL}/me/adaccounts",
            params={
                "access_token": access_token,
                "fields": "id,name,account_status"
            }
        )
        
        if ad_accounts_response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No se pudieron obtener las cuentas de anuncios"
            )
        
        ad_accounts = ad_accounts_response.json().get("data", [])
        
        # Guardar cada Ad Account
        saved_accounts = []
        for account in ad_accounts:
            account_id = account.get("id", "").replace("act_", "")
            
            # Verificar si ya existe
            existing = db.query(MetaAccount).filter(
                MetaAccount.user_id == current_user.id,
                MetaAccount.account_id == account_id
            ).first()
            
            if existing:
                # Actualizar token
                existing.access_token_encrypted = encrypt_token(access_token)
                existing.token_expires_at = datetime.utcnow() + timedelta(seconds=expires_in)
                existing.meta_user_id = meta_user_id
                existing.is_active = True
                saved_accounts.append(existing)
            else:
                # Crear nueva
                new_account = MetaAccount(
                    user_id=current_user.id,
                    meta_user_id=meta_user_id,
                    account_id=account_id,
                    account_name=account.get("name"),
                    access_token_encrypted=encrypt_token(access_token),
                    token_expires_at=datetime.utcnow() + timedelta(seconds=expires_in)
                )
                db.add(new_account)
                saved_accounts.append(new_account)
        
        db.commit()
        
        return {
            "message": f"Se conectaron {len(saved_accounts)} cuentas de anuncios",
            "accounts": [
                {"account_id": acc.account_id, "account_name": acc.account_name}
                for acc in saved_accounts
            ]
        }


# ========== CUENTAS ==========

@router.get("/accounts", response_model=List[MetaAccountResponse])
async def get_accounts(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener cuentas de Meta conectadas"""
    accounts = db.query(MetaAccount).filter(
        MetaAccount.user_id == current_user.id,
        MetaAccount.is_active == True
    ).all()
    
    return [MetaAccountResponse.model_validate(acc) for acc in accounts]


@router.delete("/accounts/{account_id}")
async def disconnect_account(
    account_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Desconectar una cuenta de Meta"""
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


# ========== MÉTRICAS ==========

@router.get("/ads")
async def get_ads_metrics(
    account_id: str,
    start_date: str,  # YYYY-MM-DD
    end_date: str,    # YYYY-MM-DD
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener métricas de anuncios de Meta"""
    
    # Buscar cuenta
    account = db.query(MetaAccount).filter(
        MetaAccount.user_id == current_user.id,
        MetaAccount.account_id == account_id,
        MetaAccount.is_active == True
    ).first()
    
    if not account:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cuenta de Meta no encontrada"
        )
    
    # Desencriptar token
    try:
        access_token = decrypt_token(account.access_token_encrypted)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token de Meta inválido, reconecta la cuenta"
        )
    
    # Consultar Meta API
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{META_BASE_URL}/act_{account_id}/insights",
            params={
                "access_token": access_token,
                "level": "ad",
                "fields": "ad_id,ad_name,spend,impressions,clicks,ctr,cpm,cpc,reach",
                "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
                "limit": 500
            }
        )
        
        if response.status_code != 200:
            error_data = response.json()
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Error de Meta API: {error_data.get('error', {}).get('message', 'Unknown')}"
            )
        
        data = response.json()
        ads = data.get("data", [])
        
        # Formatear respuesta
        result = []
        for ad in ads:
            result.append({
                "ad_id": ad.get("ad_id"),
                "ad_name": ad.get("ad_name"),
                "spend": float(ad.get("spend", 0)),
                "impressions": int(ad.get("impressions", 0)),
                "clicks": int(ad.get("clicks", 0)),
                "ctr": float(ad.get("ctr", 0)),
                "cpm": float(ad.get("cpm", 0)),
                "cpc": float(ad.get("cpc", 0)),
                "reach": int(ad.get("reach", 0))
            })
        
        return {"ads": result, "count": len(result)}


@router.get("/ads/{ad_id}")
async def get_ad_details(
    ad_id: str,
    account_id: str,
    start_date: str,
    end_date: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener detalles de un anuncio específico"""
    
    account = db.query(MetaAccount).filter(
        MetaAccount.user_id == current_user.id,
        MetaAccount.account_id == account_id,
        MetaAccount.is_active == True
    ).first()
    
    if not account:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cuenta de Meta no encontrada"
        )
    
    access_token = decrypt_token(account.access_token_encrypted)
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{META_BASE_URL}/{ad_id}/insights",
            params={
                "access_token": access_token,
                "fields": "ad_id,ad_name,spend,impressions,clicks,ctr,cpm,cpc,reach,frequency",
                "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}'
            }
        )
        
        if response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Error al obtener datos del anuncio"
            )
        
        data = response.json().get("data", [])
        
        if not data:
            return {"ad_id": ad_id, "message": "Sin datos para el rango de fechas"}
        
        return data[0]
