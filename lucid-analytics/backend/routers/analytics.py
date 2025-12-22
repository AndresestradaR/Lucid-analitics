"""
Router de Analytics
Combina datos de Meta + LucidBot (desde BD local) para calcular CPA, ROAS, etc.

CAMBIO PRINCIPAL: Ahora consulta la base de datos local en lugar de la API de LucidBot.
Esto resuelve el límite de 100 contactos y mejora la velocidad.

FIX: Ahora SIEMPRE consulta la BD local aunque no haya token de LucidBot activo.
El token solo se usa para auto-sincronizar datos faltantes.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from typing import List, Optional
from datetime import datetime, timedelta
import httpx
import logging

from database import get_db, User, MetaAccount, LucidbotConnection, LucidbotContact
from routers.auth import get_current_user
from utils import decrypt_token

router = APIRouter()
logger = logging.getLogger(__name__)

META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"
LUCIDBOT_BASE_URL = "https://panel.lucidbot.co/api"

# Colombia es UTC-5
# Las fechas en LucidBot vienen en hora Colombia (no UTC)
# Por lo tanto NO se necesita conversión de timezone
UTC_OFFSET_HOURS = 5  # No se usa, mantenido por referencia


# ========== HELPERS ==========

async def get_meta_ads_with_hierarchy(access_token: str, account_id: str, start_date: str, end_date: str):
    """Obtener métricas de Meta Ads CON jerarquía de campaña/conjunto"""
    async with httpx.AsyncClient(timeout=120) as client:
        # Paso 1: Obtener lista de anuncios con jerarquía
        ads_response = await client.get(
            f"{META_BASE_URL}/act_{account_id}/ads",
            params={
                "access_token": access_token,
                "fields": "id,name,status,campaign{id,name,daily_budget,lifetime_budget},adset{id,name,daily_budget,lifetime_budget}",
                "limit": 200
            }
        )
        
        if ads_response.status_code != 200:
            return []
        
        ads_list = ads_response.json().get("data", [])
        
        # Crear diccionario de info de ads
        ads_info = {}
        for ad in ads_list:
            ad_id = ad.get("id")
            campaign = ad.get("campaign", {})
            adset = ad.get("adset", {})
            
            daily_budget = None
            lifetime_budget = None
            
            if adset.get("daily_budget"):
                daily_budget = int(adset.get("daily_budget")) / 100
            elif campaign.get("daily_budget"):
                daily_budget = int(campaign.get("daily_budget")) / 100
                
            if adset.get("lifetime_budget"):
                lifetime_budget = int(adset.get("lifetime_budget")) / 100
            elif campaign.get("lifetime_budget"):
                lifetime_budget = int(campaign.get("lifetime_budget")) / 100
            
            ads_info[ad_id] = {
                "ad_name": ad.get("name", ""),
                "status": ad.get("status", ""),
                "campaign_id": campaign.get("id", ""),
                "campaign_name": campaign.get("name", ""),
                "adset_id": adset.get("id", ""),
                "adset_name": adset.get("name", ""),
                "daily_budget": daily_budget,
                "lifetime_budget": lifetime_budget
            }
        
        # Paso 2: Obtener insights (métricas)
        insights_response = await client.get(
            f"{META_BASE_URL}/act_{account_id}/insights",
            params={
                "access_token": access_token,
                "level": "ad",
                "fields": "ad_id,ad_name,spend,impressions,clicks,ctr,cpm,cpc,reach,actions,cost_per_action_type",
                "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
                "limit": 500
            }
        )
        
        if insights_response.status_code != 200:
            return []
        
        insights_data = insights_response.json().get("data", [])
        
        # Paso 3: Combinar insights con info de jerarquía
        result = []
        for insight in insights_data:
            ad_id = insight.get("ad_id")
            ad_info = ads_info.get(ad_id, {})
            
            messaging_conversations = 0
            cost_per_messaging = 0
            
            actions = insight.get("actions", [])
            for action in actions:
                action_type = action.get("action_type", "")
                if "messaging" in action_type.lower() or "conversation" in action_type.lower():
                    messaging_conversations += int(action.get("value", 0))
            
            cost_per_actions = insight.get("cost_per_action_type", [])
            for cpa in cost_per_actions:
                action_type = cpa.get("action_type", "")
                if "messaging" in action_type.lower() or "conversation" in action_type.lower():
                    cost_per_messaging = float(cpa.get("value", 0))
                    break
            
            result.append({
                "ad_id": ad_id,
                "ad_name": ad_info.get("ad_name") or insight.get("ad_name", ""),
                "status": ad_info.get("status", ""),
                "campaign_id": ad_info.get("campaign_id", ""),
                "campaign_name": ad_info.get("campaign_name", ""),
                "adset_id": ad_info.get("adset_id", ""),
                "adset_name": ad_info.get("adset_name", ""),
                "daily_budget": ad_info.get("daily_budget"),
                "lifetime_budget": ad_info.get("lifetime_budget"),
                "spend": insight.get("spend", "0"),
                "impressions": insight.get("impressions", "0"),
                "clicks": insight.get("clicks", "0"),
                "ctr": insight.get("ctr", "0"),
                "cpm": insight.get("cpm", "0"),
                "cpc": insight.get("cpc", "0"),
                "reach": insight.get("reach", "0"),
                "messaging_conversations": messaging_conversations,
                "cost_per_messaging": cost_per_messaging
            })
        
        return result


def get_lucidbot_data_from_db(
    db: Session,
    user_id: int,
    ad_id: str, 
    start_date: str,
    end_date: str
) -> dict:
    """
    Obtener datos de contactos desde la BASE DE DATOS LOCAL.
    
    IMPORTANTE: Las fechas en la BD están en hora Colombia (tal cual vienen de LucidBot).
    NO se necesita conversión de timezone.
    
    Args:
        db: Sesión de base de datos
        user_id: ID del usuario
        ad_id: ID del anuncio de Facebook
        start_date: Fecha inicio YYYY-MM-DD (hora Colombia)
        end_date: Fecha fin YYYY-MM-DD (hora Colombia)
    
    Returns:
        Dict con leads, sales, revenue y detalles
    """
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        
        logger.info(f"[DB QUERY] user_id={user_id}, ad_id={ad_id}, range: {start_dt} → {end_dt}")
        
        # Consultar contactos en rango
        contacts = db.query(LucidbotContact).filter(
            and_(
                LucidbotContact.user_id == user_id,
                LucidbotContact.ad_id == ad_id,
                LucidbotContact.contact_created_at >= start_dt,
                LucidbotContact.contact_created_at <= end_dt
            )
        ).all()
        
        leads = 0
        sales = 0
        revenue = 0.0
        contact_details = []
        
        for contact in contacts:
            contact_info = {
                "name": contact.full_name or "",
                "phone": contact.phone or "",
                "created_at": contact.contact_created_at.isoformat() if contact.contact_created_at else "",
                "calificacion": contact.calificacion or ""
            }
            
            if contact.total_a_pagar and contact.total_a_pagar > 0:
                sales += 1
                revenue += contact.total_a_pagar
                contact_info["is_sale"] = True
                contact_info["amount"] = contact.total_a_pagar
                contact_info["product"] = contact.producto or ""
            else:
                leads += 1
                contact_info["is_sale"] = False
            
            contact_details.append(contact_info)
        
        logger.info(f"[DB QUERY] ad_id={ad_id}: {len(contacts)} contactos, {sales} ventas, {leads} leads")
        
        return {
            "leads": leads,
            "sales": sales,
            "revenue": revenue,
            "contacts": contact_details,
            "_debug": {
                "source": "local_db",
                "total_contacts": len(contacts),
                "start": start_dt.isoformat(),
                "end": end_dt.isoformat()
            }
        }
    
    except Exception as e:
        logger.error(f"Error consultando BD: {str(e)}")
        return {"leads": 0, "sales": 0, "revenue": 0, "contacts": [], "_debug": {"error": str(e)}}


async def sync_ad_if_needed(
    db: Session,
    user_id: int,
    jwt_token: str,
    page_id: str,
    ad_id: str,
    force: bool = False
) -> bool:
    """
    Sincronizar un ad_id si no hay datos o están desactualizados.
    
    Args:
        db: Sesión de BD
        user_id: ID del usuario
        jwt_token: JWT Token de LucidBot
        page_id: Page ID de LucidBot
        ad_id: ID del anuncio
        force: Forzar sincronización aunque haya datos recientes
    
    Returns:
        True si se sincronizó, False si ya estaba actualizado
    """
    # Verificar última sincronización para este ad_id
    last_sync = db.query(func.max(LucidbotContact.synced_at)).filter(
        LucidbotContact.user_id == user_id,
        LucidbotContact.ad_id == ad_id
    ).scalar()
    
    # Si hay datos recientes (< 1 hora), no sincronizar
    if last_sync and not force:
        age = datetime.utcnow() - last_sync
        if age < timedelta(hours=1):
            logger.info(f"[SYNC] ad_id={ad_id} tiene datos recientes ({age}), saltando")
            return False
    
    logger.info(f"[SYNC] Sincronizando ad_id={ad_id}...")
    
    # Importar aquí para evitar circular import
    from routers.sync import fetch_all_contacts_for_ad, sync_contacts_to_db
    
    # Obtener todos los contactos paginando
    contacts = await fetch_all_contacts_for_ad(jwt_token, ad_id, page_id)
    
    if contacts:
        await sync_contacts_to_db(db, user_id, contacts, ad_id)
        return True
    
    return False


# ========== ENDPOINTS ==========

@router.get("/dashboard")
async def get_dashboard(
    account_id: str,
    start_date: str,
    end_date: str,
    sync: bool = True,  # Auto-sincronizar si faltan datos
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Dashboard principal: métricas combinadas Meta + LucidBot
    
    CAMBIO: Ahora usa la base de datos local para LucidBot.
    SIEMPRE consulta la BD local, aunque no haya token activo.
    El token solo se usa para auto-sincronizar datos faltantes.
    """
    
    meta_account = db.query(MetaAccount).filter(
        MetaAccount.user_id == current_user.id,
        MetaAccount.account_id == account_id,
        MetaAccount.is_active == True
    ).first()
    
    if not meta_account:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cuenta de Meta no encontrada"
        )
    
    # Verificar si hay conexión de LucidBot con token (para auto-sync)
    lucidbot_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    meta_token = decrypt_token(meta_account.access_token_encrypted)
    
    # Obtener JWT token y page_id de LucidBot (solo para auto-sync)
    jwt_token = None
    page_id = None
    if lucidbot_conn and lucidbot_conn.jwt_token_encrypted:
        jwt_token = decrypt_token(lucidbot_conn.jwt_token_encrypted)
        page_id = lucidbot_conn.page_id
    
    # Obtener anuncios de Meta CON jerarquía
    meta_ads = await get_meta_ads_with_hierarchy(meta_token, account_id, start_date, end_date)
    
    if not meta_ads:
        return {
            "message": "No hay datos de anuncios para el rango de fechas",
            "ads": [],
            "summary": {
                "total_spend": 0,
                "total_revenue": 0,
                "total_leads": 0,
                "total_sales": 0,
                "average_cpa": 0,
                "average_roas": 0,
                "profit": 0
            }
        }
    
    ads_analytics = []
    total_spend = 0
    total_revenue = 0
    total_leads = 0
    total_sales = 0
    synced_ads = []
    
    for ad in meta_ads:
        ad_id = ad.get("ad_id")
        spend = float(ad.get("spend", 0))
        
        # Verificar si hay datos en BD para este ad_id
        contact_count = db.query(func.count(LucidbotContact.id)).filter(
            LucidbotContact.user_id == current_user.id,
            LucidbotContact.ad_id == ad_id
        ).scalar()
        
        # Si no hay datos en BD y hay token activo y sync=True, intentar sincronizar
        if contact_count == 0 and jwt_token and page_id and sync:
            try:
                synced = await sync_ad_if_needed(
                    db, current_user.id, jwt_token, page_id, ad_id, force=True
                )
                if synced:
                    synced_ads.append(ad_id)
            except Exception as e:
                logger.error(f"Error sincronizando ad_id={ad_id}: {str(e)}")
        
        # SIEMPRE consultar BD local (aunque no haya token activo)
        lucid_data = get_lucidbot_data_from_db(
            db, 
            current_user.id,
            ad_id,
            start_date,
            end_date
        )
        
        leads = lucid_data["leads"]
        sales = lucid_data["sales"]
        revenue = lucid_data["revenue"]
        
        cpl = spend / leads if leads > 0 else 0
        cpa = spend / sales if sales > 0 else 0
        roas = revenue / spend if spend > 0 else 0
        conversion_rate = (sales / leads * 100) if leads > 0 else 0
        
        ad_analytics = {
            "ad_id": ad_id,
            "ad_name": ad.get("ad_name", ""),
            "campaign_id": ad.get("campaign_id", ""),
            "campaign_name": ad.get("campaign_name", ""),
            "adset_id": ad.get("adset_id", ""),
            "adset_name": ad.get("adset_name", ""),
            "daily_budget": ad.get("daily_budget"),
            "lifetime_budget": ad.get("lifetime_budget"),
            "spend": spend,
            "impressions": int(ad.get("impressions", 0)),
            "clicks": int(ad.get("clicks", 0)),
            "ctr": float(ad.get("ctr", 0)),
            "cpm": float(ad.get("cpm", 0)),
            "messaging_conversations": ad.get("messaging_conversations", 0),
            "cost_per_messaging": ad.get("cost_per_messaging", 0),
            "leads": leads,
            "sales": sales,
            "revenue": revenue,
            "cpl": round(cpl, 2),
            "cpa": round(cpa, 2),
            "roas": round(roas, 2),
            "conversion_rate": round(conversion_rate, 2)
        }
        
        ads_analytics.append(ad_analytics)
        
        total_spend += spend
        total_revenue += revenue
        total_leads += leads
        total_sales += sales
    
    ads_with_data = [a for a in ads_analytics if a["leads"] > 0 or a["sales"] > 0]
    ads_with_data.sort(key=lambda x: x["roas"], reverse=True)
    ads_analytics.sort(key=lambda x: x["spend"], reverse=True)
    
    avg_cpa = total_spend / total_sales if total_sales > 0 else 0
    avg_roas = total_revenue / total_spend if total_spend > 0 else 0
    conversion_rate = (total_sales / total_leads * 100) if total_leads > 0 else 0
    avg_cpl = total_spend / total_leads if total_leads > 0 else 0
    profit = total_revenue - total_spend
    
    return {
        "ads": ads_analytics,
        "ads_with_lucidbot_data": ads_with_data,
        "summary": {
            "total_spend": round(total_spend, 2),
            "total_revenue": round(total_revenue, 2),
            "total_leads": total_leads,
            "total_sales": total_sales,
            "average_cpa": round(avg_cpa, 2),
            "average_roas": round(avg_roas, 2),
            "average_cpl": round(avg_cpl, 2),
            "conversion_rate": round(conversion_rate, 2),
            "profit": round(profit, 2)
        },
        "date_range": {
            "start": start_date,
            "end": end_date
        },
        "_sync_info": {
            "synced_ads": synced_ads,
            "source": "local_db",
            "has_active_token": bool(jwt_token)
        }
    }


@router.get("/ad/{ad_id}/contacts")
async def get_ad_contacts(
    ad_id: str,
    start_date: str,
    end_date: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener detalle de contactos de un anuncio específico"""
    
    lucid_data = get_lucidbot_data_from_db(
        db,
        current_user.id,
        ad_id,
        start_date,
        end_date
    )
    
    return {
        "ad_id": ad_id,
        "date_range": {"start": start_date, "end": end_date},
        "leads": lucid_data["leads"],
        "sales": lucid_data["sales"],
        "revenue": lucid_data["revenue"],
        "contacts": lucid_data["contacts"],
        "_debug": lucid_data.get("_debug", {})
    }


@router.get("/chart/daily")
async def get_daily_chart(
    account_id: str,
    start_date: str,
    end_date: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener datos para gráfico diario de métricas"""
    
    meta_account = db.query(MetaAccount).filter(
        MetaAccount.user_id == current_user.id,
        MetaAccount.account_id == account_id,
        MetaAccount.is_active == True
    ).first()
    
    if not meta_account:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Cuenta de Meta no encontrada"
        )
    
    meta_token = decrypt_token(meta_account.access_token_encrypted)
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{META_BASE_URL}/act_{account_id}/insights",
            params={
                "access_token": meta_token,
                "level": "account",
                "fields": "spend,impressions,clicks,ctr,cpm",
                "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
                "time_increment": 1
            },
            timeout=60
        )
        
        if response.status_code != 200:
            return {"data": [], "error": "Error al obtener datos de Meta"}
        
        data = response.json().get("data", [])
        
        chart_data = []
        for day in data:
            chart_data.append({
                "date": day.get("date_start"),
                "spend": float(day.get("spend", 0)),
                "impressions": int(day.get("impressions", 0)),
                "clicks": int(day.get("clicks", 0)),
                "ctr": float(day.get("ctr", 0)),
                "cpm": float(day.get("cpm", 0))
            })
        
        return {"data": chart_data}


# ========== DEBUG ENDPOINTS ==========

@router.get("/debug/db-contacts/{ad_id}")
async def debug_db_contacts(
    ad_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Ver contactos en BD local para un ad_id (debug)"""
    
    contacts = db.query(LucidbotContact).filter(
        LucidbotContact.user_id == current_user.id,
        LucidbotContact.ad_id == ad_id
    ).order_by(LucidbotContact.contact_created_at.desc()).limit(50).all()
    
    return {
        "ad_id": ad_id,
        "total_in_db": len(contacts),
        "contacts": [
            {
                "lucidbot_id": c.lucidbot_id,
                "name": c.full_name,
                "phone": c.phone,
                "created_at": c.contact_created_at.isoformat() if c.contact_created_at else None,
                "total_a_pagar": c.total_a_pagar,
                "synced_at": c.synced_at.isoformat() if c.synced_at else None
            }
            for c in contacts
        ]
    }
