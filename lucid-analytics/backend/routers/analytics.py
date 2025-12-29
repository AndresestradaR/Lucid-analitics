"""
Router de Analytics - VERSION OPTIMIZADA
Combina datos de Meta + LucidBot (desde BD local) para calcular CPA, ROAS, etc.

OPTIMIZACIONES DE RENDIMIENTO (2024-12):
- Timeouts reducidos de 120s a 30s
- Cache en memoria para datos de Meta API (5 min TTL)
- Consultas batch para datos de LucidBot (elimina N+1)
- Llamadas paralelas a Meta API
- Sync en background por defecto
"""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, case
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import httpx
import logging
import asyncio
import time

from database import get_db, User, MetaAccount, LucidbotConnection, LucidbotContact
from routers.auth import get_current_user
from utils import decrypt_token

router = APIRouter()
logger = logging.getLogger(__name__)

META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"
LUCIDBOT_BASE_URL = "https://panel.lucidbot.co/api"

UTC_OFFSET_HOURS = 5

# ========== CACHE EN MEMORIA ==========
CACHE_TTL_SECONDS = 300  # 5 minutos
_meta_cache: Dict[str, Dict[str, Any]] = {}

def get_cache_key(account_id: str, start_date: str, end_date: str) -> str:
    return f"{account_id}:{start_date}:{end_date}"

def get_cached_meta_data(cache_key: str) -> Optional[List]:
    if cache_key in _meta_cache:
        entry = _meta_cache[cache_key]
        if time.time() - entry["timestamp"] < CACHE_TTL_SECONDS:
            logger.info(f"[CACHE HIT] {cache_key}")
            return entry["data"]
        else:
            del _meta_cache[cache_key]
    return None

def set_cached_meta_data(cache_key: str, data: List):
    _meta_cache[cache_key] = {"data": data, "timestamp": time.time()}
    if len(_meta_cache) > 100:
        oldest_key = min(_meta_cache.keys(), key=lambda k: _meta_cache[k]["timestamp"])
        del _meta_cache[oldest_key]


# ========== HELPERS ==========

async def get_meta_ads_with_hierarchy(access_token: str, account_id: str, start_date: str, end_date: str):
    """Obtener metricas de Meta Ads CON jerarquia - OPTIMIZADO con cache y llamadas paralelas"""
    cache_key = get_cache_key(account_id, start_date, end_date)
    cached = get_cached_meta_data(cache_key)
    if cached is not None:
        return cached

    timeout = httpx.Timeout(30.0, connect=10.0)

    async with httpx.AsyncClient(timeout=timeout) as client:
        ads_task = client.get(
            f"{META_BASE_URL}/act_{account_id}/ads",
            params={
                "access_token": access_token,
                "fields": "id,name,status,campaign{id,name,daily_budget,lifetime_budget},adset{id,name,daily_budget,lifetime_budget}",
                "limit": 200
            }
        )
        insights_task = client.get(
            f"{META_BASE_URL}/act_{account_id}/insights",
            params={
                "access_token": access_token,
                "level": "ad",
                "fields": "ad_id,ad_name,spend,impressions,clicks,ctr,cpm,cpc,reach,actions,cost_per_action_type",
                "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
                "limit": 500
            }
        )

        try:
            ads_response, insights_response = await asyncio.gather(ads_task, insights_task)
        except httpx.TimeoutException:
            logger.error(f"[META API] Timeout para cuenta {account_id}")
            return []
        except Exception as e:
            logger.error(f"[META API] Error: {str(e)}")
            return []

        if ads_response.status_code != 200:
            return []

        ads_list = ads_response.json().get("data", [])
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

        if insights_response.status_code != 200:
            return []

        insights_data = insights_response.json().get("data", [])
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

        set_cached_meta_data(cache_key, result)
        logger.info(f"[META API] Datos cacheados: {len(result)} ads")
        return result


def get_lucidbot_data_from_db(db: Session, user_id: int, ad_id: str, start_date: str, end_date: str) -> dict:
    """Obtener datos de contactos desde BD LOCAL"""
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
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
        return {
            "leads": leads, "sales": sales, "revenue": revenue,
            "contacts": contact_details,
            "_debug": {"source": "local_db", "total_contacts": len(contacts)}
        }
    except Exception as e:
        logger.error(f"Error consultando BD: {str(e)}")
        return {"leads": 0, "sales": 0, "revenue": 0, "contacts": [], "_debug": {"error": str(e)}}


def get_lucidbot_data_batch(db: Session, user_id: int, ad_ids: List[str], start_date: str, end_date: str) -> Dict[str, dict]:
    """OPTIMIZACION: Batch query para todos los ad_ids - elimina N+1"""
    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        results = db.query(
            LucidbotContact.ad_id,
            func.count(LucidbotContact.id).label('total_contacts'),
            func.sum(case((LucidbotContact.total_a_pagar > 0, 1), else_=0)).label('sales'),
            func.sum(case((LucidbotContact.total_a_pagar > 0, LucidbotContact.total_a_pagar), else_=0)).label('revenue')
        ).filter(
            and_(
                LucidbotContact.user_id == user_id,
                LucidbotContact.ad_id.in_(ad_ids),
                LucidbotContact.contact_created_at >= start_dt,
                LucidbotContact.contact_created_at <= end_dt
            )
        ).group_by(LucidbotContact.ad_id).all()

        data_by_ad = {}
        for row in results:
            total = row.total_contacts or 0
            sales = row.sales or 0
            revenue = float(row.revenue or 0)
            data_by_ad[row.ad_id] = {"leads": total - sales, "sales": sales, "revenue": revenue, "contacts": []}
        for ad_id in ad_ids:
            if ad_id not in data_by_ad:
                data_by_ad[ad_id] = {"leads": 0, "sales": 0, "revenue": 0, "contacts": []}
        logger.info(f"[BATCH] {len(ad_ids)} ad_ids, {len(results)} con datos")
        return data_by_ad
    except Exception as e:
        logger.error(f"Error batch query: {str(e)}")
        return {ad_id: {"leads": 0, "sales": 0, "revenue": 0, "contacts": []} for ad_id in ad_ids}


async def sync_ad_if_needed(db: Session, user_id: int, jwt_token: str, page_id: str, ad_id: str, force: bool = False) -> bool:
    """Sincronizar ad_id si no hay datos o estan desactualizados"""
    last_sync = db.query(func.max(LucidbotContact.synced_at)).filter(
        LucidbotContact.user_id == user_id,
        LucidbotContact.ad_id == ad_id
    ).scalar()
    if last_sync and not force:
        age = datetime.utcnow() - last_sync
        if age < timedelta(hours=1):
            return False
    logger.info(f"[SYNC] Sincronizando ad_id={ad_id}...")
    from routers.sync import fetch_all_contacts_for_ad, sync_contacts_to_db
    contacts = await fetch_all_contacts_for_ad(jwt_token, ad_id, page_id)
    if contacts:
        sync_contacts_to_db(db, user_id, contacts, ad_id)
        return True
    return False


# ========== ENDPOINTS ==========

@router.get("/dashboard")
async def get_dashboard(
    account_id: str,
    start_date: str,
    end_date: str,
    sync: bool = False,
    background_tasks: BackgroundTasks = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Dashboard principal OPTIMIZADO: cache, batch queries, timeouts reducidos"""
    start_time = time.time()

    meta_account = db.query(MetaAccount).filter(
        MetaAccount.user_id == current_user.id,
        MetaAccount.account_id == account_id,
        MetaAccount.is_active == True
    ).first()

    if not meta_account:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cuenta de Meta no encontrada")

    lucidbot_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()

    meta_token = decrypt_token(meta_account.access_token_encrypted)
    jwt_token = None
    page_id = None
    if lucidbot_conn and lucidbot_conn.jwt_token_encrypted:
        jwt_token = decrypt_token(lucidbot_conn.jwt_token_encrypted)
        page_id = lucidbot_conn.page_id

    meta_ads = await get_meta_ads_with_hierarchy(meta_token, account_id, start_date, end_date)

    if not meta_ads:
        return {
            "message": "No hay datos de anuncios para el rango de fechas",
            "ads": [],
            "summary": {"total_spend": 0, "total_revenue": 0, "total_leads": 0, "total_sales": 0,
                       "average_cpa": 0, "average_roas": 0, "profit": 0},
            "_performance": {"time_ms": int((time.time() - start_time) * 1000)}
        }

    # BATCH QUERY - elimina N+1
    ad_ids = [ad.get("ad_id") for ad in meta_ads if ad.get("ad_id")]
    lucid_data_batch = get_lucidbot_data_batch(db, current_user.id, ad_ids, start_date, end_date)

    ads_analytics = []
    total_spend = 0
    total_revenue = 0
    total_leads = 0
    total_sales = 0

    for ad in meta_ads:
        ad_id = ad.get("ad_id")
        spend = float(ad.get("spend", 0))
        lucid_data = lucid_data_batch.get(ad_id, {"leads": 0, "sales": 0, "revenue": 0})
        leads = lucid_data["leads"]
        sales = lucid_data["sales"]
        revenue = lucid_data["revenue"]
        cpl = spend / leads if leads > 0 else 0
        cpa = spend / sales if sales > 0 else 0
        roas = revenue / spend if spend > 0 else 0
        conversion_rate = (sales / leads * 100) if leads > 0 else 0

        ads_analytics.append({
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
        })
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
    elapsed_ms = int((time.time() - start_time) * 1000)
    logger.info(f"[DASHBOARD] {elapsed_ms}ms para {len(meta_ads)} ads")

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
        "date_range": {"start": start_date, "end": end_date},
        "_sync_info": {"source": "local_db_batch", "has_active_token": bool(jwt_token)},
        "_performance": {"time_ms": elapsed_ms, "total_ads": len(meta_ads)}
    }


@router.get("/ad/{ad_id}/contacts")
async def get_ad_contacts(
    ad_id: str,
    start_date: str,
    end_date: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener detalle de contactos de un anuncio especifico"""
    lucid_data = get_lucidbot_data_from_db(db, current_user.id, ad_id, start_date, end_date)
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
    """Obtener datos para grafico diario"""
    meta_account = db.query(MetaAccount).filter(
        MetaAccount.user_id == current_user.id,
        MetaAccount.account_id == account_id,
        MetaAccount.is_active == True
    ).first()

    if not meta_account:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Cuenta de Meta no encontrada")

    meta_token = decrypt_token(meta_account.access_token_encrypted)
    timeout = httpx.Timeout(30.0, connect=10.0)

    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            response = await client.get(
                f"{META_BASE_URL}/act_{account_id}/insights",
                params={
                    "access_token": meta_token,
                    "level": "account",
                    "fields": "spend,impressions,clicks,ctr,cpm",
                    "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
                    "time_increment": 1
                }
            )
        except httpx.TimeoutException:
            return {"data": [], "error": "Timeout"}

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
async def debug_db_contacts(ad_id: str, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Ver contactos en BD local para un ad_id (debug)"""
    contacts = db.query(LucidbotContact).filter(
        LucidbotContact.user_id == current_user.id,
        LucidbotContact.ad_id == ad_id
    ).order_by(LucidbotContact.contact_created_at.desc()).limit(50).all()
    return {
        "ad_id": ad_id,
        "total_in_db": len(contacts),
        "contacts": [
            {"lucidbot_id": c.lucidbot_id, "name": c.full_name, "phone": c.phone,
             "created_at": c.contact_created_at.isoformat() if c.contact_created_at else None,
             "total_a_pagar": c.total_a_pagar}
            for c in contacts
        ]
    }


@router.get("/debug/cache-stats")
async def debug_cache_stats():
    """Ver estadisticas del cache"""
    return {"entries": len(_meta_cache), "ttl_seconds": CACHE_TTL_SECONDS}


@router.delete("/debug/cache-clear")
async def debug_cache_clear():
    """Limpiar cache"""
    global _meta_cache
    count = len(_meta_cache)
    _meta_cache = {}
    return {"cleared": count}
