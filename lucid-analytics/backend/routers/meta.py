from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
import httpx
import os
from typing import Optional
from jose import jwt, JWTError

from database import get_db, MetaAccount, User
from utils import encrypt_token, decrypt_token

router = APIRouter(tags=["Meta Ads"])

META_APP_ID = os.getenv("META_APP_ID")
META_APP_SECRET = os.getenv("META_APP_SECRET")
SECRET_KEY = os.getenv("SECRET_KEY", "default-secret-key")

security = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
) -> User:
    """Obtener usuario actual del token JWT"""
    token = credentials.credentials
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        user_id = payload.get("user_id")
        if user_id is None:
            raise HTTPException(status_code=401, detail="Invalid token")
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    
    user = db.query(User).filter(User.id == user_id).first()
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")
    
    return user


@router.get("/auth-url")
async def get_auth_url(
    redirect_uri: str,
    current_user: User = Depends(get_current_user)
):
    """Obtener URL para iniciar OAuth de Meta"""
    # Scopes válidos para Marketing API
    scopes = "ads_read,ads_management,business_management"
    
    auth_url = (
        f"https://www.facebook.com/v21.0/dialog/oauth?"
        f"client_id={META_APP_ID}"
        f"&redirect_uri={redirect_uri}"
        f"&scope={scopes}"
        f"&state={current_user.id}"
    )
    
    return {"auth_url": auth_url}


@router.get("/callback")
async def oauth_callback(
    code: Optional[str] = None,
    error: Optional[str] = None,
    error_message: Optional[str] = None,
    error_reason: Optional[str] = None,
    state: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """Callback de OAuth de Meta - recibe el código de autorización"""
    
    # Si hay error, retornarlo
    if error:
        return {
            "success": False,
            "error": error,
            "error_message": error_message or error_reason,
            "message": "Error en la autorización de Facebook"
        }
    
    if not code:
        raise HTTPException(status_code=400, detail="No authorization code received")
    
    if not state:
        raise HTTPException(status_code=400, detail="No state parameter received")
    
    try:
        user_id = int(state)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid state parameter")
    
    # Obtener el usuario
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Intercambiar código por token de acceso
    redirect_uri = "https://api.lucidestrategasia.online/api/meta/callback"
    
    token_url = "https://graph.facebook.com/v21.0/oauth/access_token"
    params = {
        "client_id": META_APP_ID,
        "client_secret": META_APP_SECRET,
        "redirect_uri": redirect_uri,
        "code": code
    }
    
    async with httpx.AsyncClient() as client:
        # Obtener token de corta duración
        response = await client.get(token_url, params=params)
        
        if response.status_code != 200:
            error_data = response.json()
            return {
                "success": False,
                "error": "token_exchange_failed",
                "details": error_data
            }
        
        token_data = response.json()
        short_lived_token = token_data.get("access_token")
        
        # Intercambiar por token de larga duración (60 días)
        long_lived_url = "https://graph.facebook.com/v21.0/oauth/access_token"
        long_lived_params = {
            "grant_type": "fb_exchange_token",
            "client_id": META_APP_ID,
            "client_secret": META_APP_SECRET,
            "fb_exchange_token": short_lived_token
        }
        
        long_response = await client.get(long_lived_url, params=long_lived_params)
        
        if long_response.status_code == 200:
            long_data = long_response.json()
            access_token = long_data.get("access_token")
            expires_in = long_data.get("expires_in", 5184000)  # 60 días por defecto
        else:
            # Si falla, usar el token de corta duración
            access_token = short_lived_token
            expires_in = 3600
        
        # Obtener cuentas de anuncios del usuario
        ad_accounts_url = "https://graph.facebook.com/v21.0/me/adaccounts"
        ad_params = {
            "access_token": access_token,
            "fields": "id,name,account_id,currency,timezone_name"
        }
        
        accounts_response = await client.get(ad_accounts_url, params=ad_params)
        
        if accounts_response.status_code != 200:
            return {
                "success": False,
                "error": "failed_to_get_ad_accounts",
                "details": accounts_response.json()
            }
        
        accounts_data = accounts_response.json()
        accounts = accounts_data.get("data", [])
        
        # Guardar cada cuenta de anuncios
        saved_accounts = []
        for account in accounts:
            account_id = account.get("account_id")
            
            # Verificar si ya existe
            existing = db.query(MetaAccount).filter(
                MetaAccount.user_id == user_id,
                MetaAccount.account_id == account_id
            ).first()
            
            if existing:
                # Actualizar token
                existing.access_token_encrypted = encrypt_token(access_token)
                existing.account_name = account.get("name", "")
                existing.is_active = True
            else:
                # Crear nueva
                new_account = MetaAccount(
                    user_id=user_id,
                    account_id=account_id,
                    account_name=account.get("name", ""),
                    access_token_encrypted=encrypt_token(access_token),
                    is_active=True
                )
                db.add(new_account)
            
            saved_accounts.append({
                "account_id": account_id,
                "name": account.get("name", "")
            })
        
        db.commit()
        
        return {
            "success": True,
            "message": f"Conectadas {len(saved_accounts)} cuentas de anuncios",
            "accounts": saved_accounts
        }


@router.get("/accounts")
async def get_accounts(
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
                "id": acc.id,
                "account_id": acc.account_id,
                "account_name": acc.account_name,
                "is_active": acc.is_active
            }
            for acc in accounts
        ]
    }


@router.delete("/accounts/{account_id}")
async def disconnect_account(
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
        raise HTTPException(status_code=404, detail="Account not found")
    
    account.is_active = False
    db.commit()
    
    return {"message": "Account disconnected"}


@router.get("/ads")
async def get_ads(
    account_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener anuncios con métricas de una cuenta"""
    account = db.query(MetaAccount).filter(
        MetaAccount.user_id == current_user.id,
        MetaAccount.account_id == account_id,
        MetaAccount.is_active == True
    ).first()
    
    if not account:
        raise HTTPException(status_code=404, detail="Account not found or not connected")
    
    access_token = decrypt_token(account.access_token_encrypted)
    
    # Construir rango de fechas
    time_range = ""
    if start_date and end_date:
        time_range = f"{{'since':'{start_date}','until':'{end_date}'}}"
    
    # Obtener anuncios con insights - AGREGADO: campaign y adset con budget
    ads_url = f"https://graph.facebook.com/v21.0/act_{account_id}/ads"
    params = {
        "access_token": access_token,
        "fields": "id,name,status,creative{thumbnail_url},campaign{id,name,daily_budget,lifetime_budget},adset{id,name,daily_budget,lifetime_budget}",
        "limit": 100
    }
    
    async with httpx.AsyncClient() as client:
        response = await client.get(ads_url, params=params)
        
        if response.status_code != 200:
            error_data = response.json()
            raise HTTPException(status_code=400, detail=error_data)
        
        ads_data = response.json()
        ads = ads_data.get("data", [])
        
        # Para cada anuncio, obtener insights
        result_ads = []
        for ad in ads:
            ad_id = ad.get("id")
            
            # Obtener insights del anuncio
            insights_url = f"https://graph.facebook.com/v21.0/{ad_id}/insights"
            insights_params = {
                "access_token": access_token,
                "fields": "spend,impressions,clicks,ctr,cpm,cpc,reach,frequency,actions,cost_per_action_type",
                "date_preset": "last_30d" if not start_date else None
            }
            
            if start_date and end_date:
                insights_params["time_range"] = f"{{'since':'{start_date}','until':'{end_date}'}}"
            
            insights_response = await client.get(insights_url, params={k: v for k, v in insights_params.items() if v})
            
            metrics = {}
            messaging_conversations = 0
            cost_per_messaging = 0
            
            if insights_response.status_code == 200:
                insights_data = insights_response.json()
                if insights_data.get("data"):
                    metrics = insights_data["data"][0]
                    
                    # Buscar conversaciones de mensajes en actions
                    actions = metrics.get("actions", [])
                    for action in actions:
                        action_type = action.get("action_type", "")
                        if "messaging" in action_type.lower() or "conversation" in action_type.lower():
                            messaging_conversations += int(action.get("value", 0))
                    
                    # Buscar costo por conversación en cost_per_action_type
                    cost_per_actions = metrics.get("cost_per_action_type", [])
                    for cpa in cost_per_actions:
                        action_type = cpa.get("action_type", "")
                        if "messaging" in action_type.lower() or "conversation" in action_type.lower():
                            cost_per_messaging = float(cpa.get("value", 0))
                            break
            
            # Extraer campaign y adset info
            campaign_data = ad.get("campaign", {})
            adset_data = ad.get("adset", {})
            
            # Obtener presupuesto (viene en centavos, dividir por 100)
            adset_daily_budget = adset_data.get("daily_budget")
            adset_lifetime_budget = adset_data.get("lifetime_budget")
            campaign_daily_budget = campaign_data.get("daily_budget")
            campaign_lifetime_budget = campaign_data.get("lifetime_budget")
            
            # Usar el presupuesto del adset primero, si no existe usar el de campaña
            daily_budget = None
            if adset_daily_budget:
                daily_budget = int(adset_daily_budget) / 100
            elif campaign_daily_budget:
                daily_budget = int(campaign_daily_budget) / 100
                
            lifetime_budget = None
            if adset_lifetime_budget:
                lifetime_budget = int(adset_lifetime_budget) / 100
            elif campaign_lifetime_budget:
                lifetime_budget = int(campaign_lifetime_budget) / 100
            
            result_ads.append({
                "ad_id": ad_id,
                "ad_name": ad.get("name", ""),
                "status": ad.get("status", ""),
                "thumbnail_url": ad.get("creative", {}).get("thumbnail_url", ""),
                # Jerarquía
                "campaign_id": campaign_data.get("id", ""),
                "campaign_name": campaign_data.get("name", ""),
                "adset_id": adset_data.get("id", ""),
                "adset_name": adset_data.get("name", ""),
                # Presupuesto
                "daily_budget": daily_budget,
                "lifetime_budget": lifetime_budget,
                # Métricas básicas
                "spend": float(metrics.get("spend", 0)),
                "impressions": int(metrics.get("impressions", 0)),
                "clicks": int(metrics.get("clicks", 0)),
                "ctr": float(metrics.get("ctr", 0)),
                "cpm": float(metrics.get("cpm", 0)),
                "cpc": float(metrics.get("cpc", 0)),
                "reach": int(metrics.get("reach", 0)),
                "frequency": float(metrics.get("frequency", 0)),
                # Métricas de mensajes
                "messaging_conversations": messaging_conversations,
                "cost_per_messaging": cost_per_messaging
            })
        
        return {"ads": result_ads}


@router.get("/ads/{ad_id}")
async def get_ad_details(
    ad_id: str,
    account_id: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
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
        raise HTTPException(status_code=404, detail="Account not found or not connected")
    
    access_token = decrypt_token(account.access_token_encrypted)
    
    async with httpx.AsyncClient() as client:
        # Obtener detalles del anuncio - AGREGADO: campaign y adset
        ad_url = f"https://graph.facebook.com/v21.0/{ad_id}"
        ad_params = {
            "access_token": access_token,
            "fields": "id,name,status,creative{thumbnail_url,body,title},campaign{id,name},adset{id,name}"
        }
        
        ad_response = await client.get(ad_url, params=ad_params)
        
        if ad_response.status_code != 200:
            raise HTTPException(status_code=400, detail=ad_response.json())
        
        ad_data = ad_response.json()
        
        # Obtener insights
        insights_url = f"https://graph.facebook.com/v21.0/{ad_id}/insights"
        insights_params = {
            "access_token": access_token,
            "fields": "spend,impressions,clicks,ctr,cpm,cpc,reach,frequency,actions,cost_per_action_type"
        }
        
        if start_date and end_date:
            insights_params["time_range"] = f"{{'since':'{start_date}','until':'{end_date}'}}"
        else:
            insights_params["date_preset"] = "last_30d"
        
        insights_response = await client.get(insights_url, params=insights_params)
        
        metrics = {}
        if insights_response.status_code == 200:
            insights_data = insights_response.json()
            if insights_data.get("data"):
                metrics = insights_data["data"][0]
        
        # Extraer campaign y adset info
        campaign_data = ad_data.get("campaign", {})
        adset_data = ad_data.get("adset", {})
        
        return {
            "ad_id": ad_data.get("id"),
            "ad_name": ad_data.get("name", ""),
            "status": ad_data.get("status", ""),
            "creative": ad_data.get("creative", {}),
            # NUEVOS CAMPOS
            "campaign_id": campaign_data.get("id", ""),
            "campaign_name": campaign_data.get("name", ""),
            "adset_id": adset_data.get("id", ""),
            "adset_name": adset_data.get("name", ""),
            "metrics": {
                "spend": float(metrics.get("spend", 0)),
                "impressions": int(metrics.get("impressions", 0)),
                "clicks": int(metrics.get("clicks", 0)),
                "ctr": float(metrics.get("ctr", 0)),
                "cpm": float(metrics.get("cpm", 0)),
                "cpc": float(metrics.get("cpc", 0)),
                "reach": int(metrics.get("reach", 0)),
                "frequency": float(metrics.get("frequency", 0)),
                "actions": metrics.get("actions", []),
                "cost_per_action": metrics.get("cost_per_action_type", [])
            }
        }
