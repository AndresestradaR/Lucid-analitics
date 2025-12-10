"""
Router de Analytics
Combina datos de Meta + LucidBot para calcular CPA, ROAS, etc.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
import httpx

from database import get_db, User, MetaAccount, LucidbotConnection, AdMetricsCache
from schemas import AdAnalytics, DashboardSummary, ChartDataPoint
from routers.auth import get_current_user
from utils import decrypt_token

router = APIRouter()

META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"
LUCIDBOT_BASE_URL = "https://panel.lucidbot.co/api"


# ========== HELPERS ==========

async def get_meta_ads(access_token: str, account_id: str, start_date: str, end_date: str):
    """Obtener métricas de Meta Ads"""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{META_BASE_URL}/act_{account_id}/insights",
            params={
                "access_token": access_token,
                "level": "ad",
                "fields": "ad_id,ad_name,spend,impressions,clicks,ctr,cpm,cpc,reach",
                "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
                "limit": 500
            },
            timeout=60
        )
        
        if response.status_code != 200:
            return []
        
        return response.json().get("data", [])


async def get_lucidbot_contacts_by_ad(api_token: str, ad_id: str, ad_field_id: str = "728462"):
    """Obtener contactos de LucidBot por Ad ID"""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{LUCIDBOT_BASE_URL}/contacts/find_by_custom_field",
            headers={
                "X-ACCESS-TOKEN": api_token,
                "Accept": "application/json"
            },
            params={
                "field_id": ad_field_id,
                "value": ad_id
            },
            timeout=30
        )
        
        if response.status_code != 200:
            return {"leads": 0, "sales": 0, "revenue": 0}
        
        contacts = response.json().get("data", [])
        
        leads = 0
        sales = 0
        revenue = 0
        
        for contact in contacts:
            custom_fields = contact.get("custom_fields", {})
            total_paid = custom_fields.get("Total a pagar")
            
            if total_paid:
                try:
                    amount = float(total_paid)
                    sales += 1
                    revenue += amount
                except ValueError:
                    leads += 1
            else:
                leads += 1
        
        return {"leads": leads, "sales": sales, "revenue": revenue}


# ========== ENDPOINTS ==========

@router.get("/dashboard")
async def get_dashboard(
    account_id: str,
    start_date: str,  # YYYY-MM-DD
    end_date: str,    # YYYY-MM-DD
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Dashboard principal: métricas combinadas Meta + LucidBot
    """
    
    # Obtener cuenta de Meta
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
    
    # Obtener conexión de LucidBot
    lucidbot_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not lucidbot_conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conexión de LucidBot no encontrada"
        )
    
    # Desencriptar tokens
    meta_token = decrypt_token(meta_account.access_token_encrypted)
    lucidbot_token = decrypt_token(lucidbot_conn.api_token_encrypted)
    
    # Obtener anuncios de Meta
    meta_ads = await get_meta_ads(meta_token, account_id, start_date, end_date)
    
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
                "average_roas": 0
            }
        }
    
    # Combinar con datos de LucidBot
    ads_analytics = []
    total_spend = 0
    total_revenue = 0
    total_leads = 0
    total_sales = 0
    
    for ad in meta_ads:
        ad_id = ad.get("ad_id")
        spend = float(ad.get("spend", 0))
        
        # Obtener datos de LucidBot para este anuncio
        lucid_data = await get_lucidbot_contacts_by_ad(lucidbot_token, ad_id)
        
        leads = lucid_data["leads"]
        sales = lucid_data["sales"]
        revenue = lucid_data["revenue"]
        
        # Calcular métricas
        cpl = spend / leads if leads > 0 else 0
        cpa = spend / sales if sales > 0 else 0
        roas = revenue / spend if spend > 0 else 0
        
        ad_analytics = {
            "ad_id": ad_id,
            "ad_name": ad.get("ad_name", ""),
            "spend": spend,
            "impressions": int(ad.get("impressions", 0)),
            "clicks": int(ad.get("clicks", 0)),
            "ctr": float(ad.get("ctr", 0)),
            "cpm": float(ad.get("cpm", 0)),
            "leads": leads,
            "sales": sales,
            "revenue": revenue,
            "cpl": round(cpl, 2),
            "cpa": round(cpa, 2),
            "roas": round(roas, 2)
        }
        
        ads_analytics.append(ad_analytics)
        
        total_spend += spend
        total_revenue += revenue
        total_leads += leads
        total_sales += sales
    
    # Ordenar por ROAS descendente
    ads_analytics.sort(key=lambda x: x["roas"], reverse=True)
    
    # Calcular promedios
    avg_cpa = total_spend / total_sales if total_sales > 0 else 0
    avg_roas = total_revenue / total_spend if total_spend > 0 else 0
    conversion_rate = (total_sales / total_leads * 100) if total_leads > 0 else 0
    
    return {
        "ads": ads_analytics,
        "summary": {
            "total_spend": round(total_spend, 2),
            "total_revenue": round(total_revenue, 2),
            "total_leads": total_leads,
            "total_sales": total_sales,
            "average_cpa": round(avg_cpa, 2),
            "average_roas": round(avg_roas, 2),
            "conversion_rate": round(conversion_rate, 2)
        },
        "top_ads": ads_analytics[:10],
        "worst_ads": list(reversed(ads_analytics[-10:])) if len(ads_analytics) >= 10 else []
    }


@router.get("/ad/{ad_id}")
async def get_ad_analytics(
    ad_id: str,
    account_id: str,
    start_date: str,
    end_date: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener métricas detalladas de un anuncio específico"""
    
    # Obtener tokens
    meta_account = db.query(MetaAccount).filter(
        MetaAccount.user_id == current_user.id,
        MetaAccount.account_id == account_id,
        MetaAccount.is_active == True
    ).first()
    
    lucidbot_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not meta_account or not lucidbot_conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Faltan conexiones de Meta o LucidBot"
        )
    
    meta_token = decrypt_token(meta_account.access_token_encrypted)
    lucidbot_token = decrypt_token(lucidbot_conn.api_token_encrypted)
    
    # Obtener datos de Meta
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{META_BASE_URL}/{ad_id}/insights",
            params={
                "access_token": meta_token,
                "fields": "ad_id,ad_name,spend,impressions,clicks,ctr,cpm,cpc,reach,frequency",
                "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}'
            },
            timeout=30
        )
        
        if response.status_code != 200:
            meta_data = {}
        else:
            data = response.json().get("data", [])
            meta_data = data[0] if data else {}
    
    # Obtener datos de LucidBot
    lucid_data = await get_lucidbot_contacts_by_ad(lucidbot_token, ad_id)
    
    spend = float(meta_data.get("spend", 0))
    leads = lucid_data["leads"]
    sales = lucid_data["sales"]
    revenue = lucid_data["revenue"]
    
    return {
        "ad_id": ad_id,
        "ad_name": meta_data.get("ad_name", ""),
        "meta_metrics": {
            "spend": spend,
            "impressions": int(meta_data.get("impressions", 0)),
            "clicks": int(meta_data.get("clicks", 0)),
            "ctr": float(meta_data.get("ctr", 0)),
            "cpm": float(meta_data.get("cpm", 0)),
            "cpc": float(meta_data.get("cpc", 0)),
            "reach": int(meta_data.get("reach", 0)),
            "frequency": float(meta_data.get("frequency", 0))
        },
        "lucidbot_metrics": {
            "leads": leads,
            "sales": sales,
            "revenue": revenue
        },
        "calculated_metrics": {
            "cpl": round(spend / leads, 2) if leads > 0 else 0,
            "cpa": round(spend / sales, 2) if sales > 0 else 0,
            "roas": round(revenue / spend, 2) if spend > 0 else 0,
            "conversion_rate": round(sales / leads * 100, 2) if leads > 0 else 0
        }
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
    
    # Obtener datos diarios de Meta
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{META_BASE_URL}/act_{account_id}/insights",
            params={
                "access_token": meta_token,
                "level": "account",
                "fields": "spend,impressions,clicks,ctr,cpm",
                "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}',
                "time_increment": 1  # Daily breakdown
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


@router.get("/compare-ads")
async def compare_ads(
    account_id: str,
    ad_ids: str,  # Comma-separated list of ad IDs
    start_date: str,
    end_date: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Comparar múltiples anuncios"""
    
    ad_id_list = [x.strip() for x in ad_ids.split(",")]
    
    if len(ad_id_list) > 20:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Máximo 20 anuncios para comparar"
        )
    
    results = []
    
    for ad_id in ad_id_list:
        ad_data = await get_ad_analytics(
            ad_id=ad_id,
            account_id=account_id,
            start_date=start_date,
            end_date=end_date,
            current_user=current_user,
            db=db
        )
        results.append(ad_data)
    
    # Ordenar por ROAS
    results.sort(key=lambda x: x["calculated_metrics"]["roas"], reverse=True)
    
    return {"ads": results, "count": len(results)}
