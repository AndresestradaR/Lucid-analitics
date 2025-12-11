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
            
            # Calcular presupuesto
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
            
            # Extraer messaging conversations
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


async def get_lucidbot_contacts_by_ad(api_token: str, ad_id: str, ad_field_id: str = "728462"):
    """Obtener contactos de LucidBot por Ad ID"""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{LUCIDBOT_BASE_URL}/users/find_by_custom_field",
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
            return {"leads": 0, "sales": 0, "revenue": 0, "contacts": []}
        
        contacts = response.json().get("data", [])
        
        leads = 0
        sales = 0
        revenue = 0
        contact_details = []
        
        for contact in contacts:
            custom_fields = contact.get("custom_fields", {})
            total_paid = custom_fields.get("Total a pagar")
            
            contact_info = {
                "name": contact.get("full_name", ""),
                "phone": contact.get("phone", ""),
                "created_at": contact.get("created_at", ""),
                "calificacion": custom_fields.get("Calificacion_LucidSales", "")
            }
            
            if total_paid:
                try:
                    amount = float(total_paid)
                    sales += 1
                    revenue += amount
                    contact_info["is_sale"] = True
                    contact_info["amount"] = amount
                    contact_info["product"] = custom_fields.get("Producto_Ordenados", "")
                except ValueError:
                    leads += 1
                    contact_info["is_sale"] = False
            else:
                leads += 1
                contact_info["is_sale"] = False
            
            contact_details.append(contact_info)
        
        return {
            "leads": leads, 
            "sales": sales, 
            "revenue": revenue,
            "contacts": contact_details
        }


# ========== ENDPOINTS ==========

@router.get("/dashboard")
async def get_dashboard(
    account_id: str,
    start_date: str,
    end_date: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Dashboard principal: métricas combinadas Meta + LucidBot"""
    
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
    
    lucidbot_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not lucidbot_conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conexión de LucidBot no encontrada"
        )
    
    meta_token = decrypt_token(meta_account.access_token_encrypted)
    lucidbot_token = decrypt_token(lucidbot_conn.api_token_encrypted)
    
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
    
    for ad in meta_ads:
        ad_id = ad.get("ad_id")
        spend = float(ad.get("spend", 0))
        
        lucid_data = await get_lucidbot_contacts_by_ad(lucidbot_token, ad_id)
        
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
        }
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
    """Análisis detallado de un anuncio específico"""
    
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
    
    lucidbot_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not lucidbot_conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conexión de LucidBot no encontrada"
        )
    
    meta_token = decrypt_token(meta_account.access_token_encrypted)
    lucidbot_token = decrypt_token(lucidbot_conn.api_token_encrypted)
    
    async with httpx.AsyncClient() as client:
        ad_response = await client.get(
            f"{META_BASE_URL}/{ad_id}",
            params={
                "access_token": meta_token,
                "fields": "id,name,status,campaign{id,name},adset{id,name}"
            },
            timeout=30
        )
        
        ad_info = {}
        if ad_response.status_code == 200:
            ad_data = ad_response.json()
            campaign = ad_data.get("campaign", {})
            adset = ad_data.get("adset", {})
            ad_info = {
                "ad_name": ad_data.get("name", ""),
                "campaign_id": campaign.get("id", ""),
                "campaign_name": campaign.get("name", ""),
                "adset_id": adset.get("id", ""),
                "adset_name": adset.get("name", "")
            }
        
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
    
    lucid_data = await get_lucidbot_contacts_by_ad(lucidbot_token, ad_id)
    
    spend = float(meta_data.get("spend", 0))
    leads = lucid_data["leads"]
    sales = lucid_data["sales"]
    revenue = lucid_data["revenue"]
    
    return {
        "ad_id": ad_id,
        "ad_name": ad_info.get("ad_name") or meta_data.get("ad_name", ""),
        "campaign_id": ad_info.get("campaign_id", ""),
        "campaign_name": ad_info.get("campaign_name", ""),
        "adset_id": ad_info.get("adset_id", ""),
        "adset_name": ad_info.get("adset_name", ""),
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
            "revenue": revenue,
            "contacts": lucid_data.get("contacts", [])
        },
        "calculated_metrics": {
            "cpl": round(spend / leads, 2) if leads > 0 else 0,
            "cpa": round(spend / sales, 2) if sales > 0 else 0,
            "roas": round(revenue / spend, 2) if spend > 0 else 0,
            "conversion_rate": round(sales / leads * 100, 2) if leads > 0 else 0,
            "profit": round(revenue - spend, 2)
        },
        "date_range": {
            "start": start_date,
            "end": end_date
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


@router.get("/compare-ads")
async def compare_ads(
    account_id: str,
    ad_ids: str,
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
    
    results.sort(key=lambda x: x["calculated_metrics"]["roas"], reverse=True)
    
    return {"ads": results, "count": len(results)}


@router.get("/quick-stats")
async def get_quick_stats(
    account_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Estadísticas rápidas: últimos 7 días vs 7 días anteriores"""
    
    today = datetime.now()
    last_7_days_end = today.strftime("%Y-%m-%d")
    last_7_days_start = (today - timedelta(days=7)).strftime("%Y-%m-%d")
    prev_7_days_end = (today - timedelta(days=8)).strftime("%Y-%m-%d")
    prev_7_days_start = (today - timedelta(days=15)).strftime("%Y-%m-%d")
    
    current_data = await get_dashboard(
        account_id=account_id,
        start_date=last_7_days_start,
        end_date=last_7_days_end,
        current_user=current_user,
        db=db
    )
    
    previous_data = await get_dashboard(
        account_id=account_id,
        start_date=prev_7_days_start,
        end_date=prev_7_days_end,
        current_user=current_user,
        db=db
    )
    
    current_summary = current_data.get("summary", {})
    previous_summary = previous_data.get("summary", {})
    
    def calc_change(current, previous):
        if previous == 0:
            return 100 if current > 0 else 0
        return round(((current - previous) / previous) * 100, 1)
    
    return {
        "current_period": {
            "start": last_7_days_start,
            "end": last_7_days_end,
            "metrics": current_summary
        },
        "previous_period": {
            "start": prev_7_days_start,
            "end": prev_7_days_end,
            "metrics": previous_summary
        },
        "changes": {
            "spend_change": calc_change(
                current_summary.get("total_spend", 0),
                previous_summary.get("total_spend", 0)
            ),
            "revenue_change": calc_change(
                current_summary.get("total_revenue", 0),
                previous_summary.get("total_revenue", 0)
            ),
            "leads_change": calc_change(
                current_summary.get("total_leads", 0),
                previous_summary.get("total_leads", 0)
            ),
            "sales_change": calc_change(
                current_summary.get("total_sales", 0),
                previous_summary.get("total_sales", 0)
            ),
            "cpa_change": calc_change(
                current_summary.get("average_cpa", 0),
                previous_summary.get("average_cpa", 0)
            ),
            "roas_change": calc_change(
                current_summary.get("average_roas", 0),
                previous_summary.get("average_roas", 0)
            )
        }
    }
