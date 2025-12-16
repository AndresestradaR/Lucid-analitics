"""
Router de Analytics
Combina datos de Meta + LucidBot para calcular CPA, ROAS, etc.

VERSIÓN CORREGIDA - 16 dic 2025
- Fix: created_at de LucidBot YA está en hora Colombia, NO aplicar offset UTC
- Fix: Removido campo "Contact" inexistente
- Agregados endpoints de debug para verificar datos
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
import httpx
import re
import logging

from database import get_db, User, MetaAccount, LucidbotConnection
from routers.auth import get_current_user
from utils import decrypt_token

router = APIRouter()

META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"
LUCIDBOT_BASE_URL = "https://panel.lucidbot.co/api"

# Configurar logging para debug
logger = logging.getLogger(__name__)


# ========== HELPERS ==========

def parse_lucidbot_date(date_str: str) -> Optional[datetime]:
    """
    Parsear fecha de LucidBot a datetime
    
    Formatos observados en LucidBot API:
    - "2025-12-13 21:21:03" (formato created_at - hora Colombia)
    - "2025-12-13T21:21:03+00:00" (formato subscribed_date - ISO)
    - "13/12/2025 17:08" (formato legacy)
    """
    if not date_str:
        return None
    
    # Limpiar el string
    date_str = str(date_str).strip()
    
    # Remover timezone info si existe (para comparar como naive datetime)
    # Formato ISO con timezone: "2025-12-13T21:21:03+00:00"
    if '+' in date_str:
        date_str = date_str.split('+')[0]
    if 'Z' in date_str:
        date_str = date_str.replace('Z', '')
    
    # Formatos a probar
    formats = [
        "%Y-%m-%d %H:%M:%S",       # "2025-12-13 21:21:03" - formato principal de created_at
        "%Y-%m-%dT%H:%M:%S",       # "2025-12-13T21:21:03" - ISO sin timezone
        "%Y-%m-%d %H:%M:%S.%f",    # Con microsegundos
        "%Y-%m-%dT%H:%M:%S.%f",    # ISO con microsegundos
        "%Y-%m-%d",                # Solo fecha
    ]
    
    for fmt in formats:
        try:
            parsed = datetime.strptime(date_str, fmt)
            return parsed
        except ValueError:
            continue
    
    # Parseo manual para formato DD/MM/YYYY H:MM (legacy)
    pattern_datetime = r'^(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2}):(\d{2})(?::(\d{2}))?$'
    match = re.match(pattern_datetime, date_str)
    
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year = int(match.group(3))
        hour = int(match.group(4))
        minute = int(match.group(5))
        second = int(match.group(6)) if match.group(6) else 0
        
        try:
            return datetime(year, month, day, hour, minute, second)
        except ValueError:
            pass
    
    # Solo fecha DD/MM/YYYY
    pattern_date = r'^(\d{1,2})/(\d{1,2})/(\d{4})$'
    match = re.match(pattern_date, date_str)
    
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year = int(match.group(3))
        
        try:
            return datetime(year, month, day, 0, 0, 0)
        except ValueError:
            pass
    
    logger.warning(f"No se pudo parsear fecha: {date_str}")
    return None


def is_date_in_range(date_str: str, start_date: str, end_date: str) -> bool:
    """
    Verificar si una fecha está dentro del rango
    
    IMPORTANTE: El campo created_at de LucidBot YA está en hora Colombia.
    NO se aplica ajuste de timezone.
    
    Args:
        date_str: Fecha del contacto (ej: "2025-12-13 21:21:03") - hora Colombia
        start_date: Fecha inicio YYYY-MM-DD (ej: "2025-12-13")
        end_date: Fecha fin YYYY-MM-DD (ej: "2025-12-13")
    
    Returns:
        True si la fecha está en el rango, False si no
    """
    contact_date = parse_lucidbot_date(date_str)
    if not contact_date:
        return False
    
    try:
        # NO aplicar ajuste de timezone - created_at ya está en hora Colombia
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        
        in_range = start <= contact_date <= end
        
        return in_range
    except ValueError as e:
        logger.error(f"Error parseando fechas de rango: {e}")
        return False


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
            logger.error(f"Error obteniendo ads de Meta: {ads_response.status_code}")
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
            logger.error(f"Error obteniendo insights de Meta: {insights_response.status_code}")
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
                "spend": float(insight.get("spend", 0)),
                "impressions": int(insight.get("impressions", 0)),
                "clicks": int(insight.get("clicks", 0)),
                "ctr": float(insight.get("ctr", 0)),
                "cpm": float(insight.get("cpm", 0)),
                "cpc": float(insight.get("cpc", 0)) if insight.get("cpc") else 0,
                "reach": int(insight.get("reach", 0)),
                "messaging_conversations": messaging_conversations,
                "cost_per_messaging": cost_per_messaging
            })
        
        return result


async def get_lucidbot_contacts_by_ad(
    api_token: str, 
    ad_id: str, 
    start_date: str,
    end_date: str,
    ad_field_id: str = "728462"
):
    """
    Obtener contactos de LucidBot por Ad ID FILTRADOS POR FECHA
    
    IMPORTANTE: El campo created_at de LucidBot YA está en hora Colombia.
    No se aplica ajuste de timezone.
    
    Args:
        api_token: Token de API de LucidBot
        ad_id: ID del anuncio de Facebook
        start_date: Fecha inicio (YYYY-MM-DD) en hora Colombia
        end_date: Fecha fin (YYYY-MM-DD) en hora Colombia
        ad_field_id: ID del campo personalizado donde está el Ad ID (default: 728462)
    """
    async with httpx.AsyncClient() as client:
        try:
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
        except httpx.TimeoutException:
            logger.error(f"Timeout obteniendo contactos de LucidBot para ad_id={ad_id}")
            return {"leads": 0, "sales": 0, "revenue": 0, "contacts": [], "_debug": {"error": "timeout"}}
        except Exception as e:
            logger.error(f"Error obteniendo contactos de LucidBot: {e}")
            return {"leads": 0, "sales": 0, "revenue": 0, "contacts": [], "_debug": {"error": str(e)}}
        
        if response.status_code != 200:
            logger.warning(f"LucidBot API retornó status {response.status_code} para ad_id={ad_id}")
            return {"leads": 0, "sales": 0, "revenue": 0, "contacts": [], "_debug": {"error": f"status_{response.status_code}"}}
        
        contacts = response.json().get("data", [])
        
        leads = 0
        sales = 0
        revenue = 0
        contact_details = []
        
        # Contadores para debug
        total_contacts = len(contacts)
        filtered_out = 0
        no_date = 0
        
        logger.info(f"ad_id={ad_id}: {total_contacts} contactos encontrados, filtrando por {start_date} a {end_date}")
        
        for contact in contacts:
            # OBTENER FECHA DEL CONTACTO
            # El campo created_at está en el nivel raíz y YA está en hora Colombia
            contact_date = contact.get("created_at")
            
            # Si no hay fecha, saltar este contacto
            if not contact_date:
                no_date += 1
                filtered_out += 1
                continue
            
            # FILTRAR POR FECHA - SIN ajuste de timezone (created_at ya es hora Colombia)
            if not is_date_in_range(str(contact_date), start_date, end_date):
                filtered_out += 1
                continue
            
            # Obtener custom_fields (es un diccionario)
            custom_fields = contact.get("custom_fields", {})
            
            # Buscar "Total a pagar" para determinar si es venta
            total_paid = custom_fields.get("Total a pagar")
            
            contact_info = {
                "name": contact.get("full_name", ""),
                "phone": contact.get("phone", ""),
                "created_at": contact_date,
                "estado_pedido": custom_fields.get("Estado_pedido", ""),
                "ciudad": custom_fields.get("Ciudad", "")
            }
            
            if total_paid:
                try:
                    amount = float(str(total_paid).replace(",", "").replace("$", "").strip())
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
        
        logger.info(f"ad_id={ad_id}: {len(contact_details)} contactos en rango, {sales} ventas, {leads} leads")
        
        return {
            "leads": leads, 
            "sales": sales, 
            "revenue": revenue,
            "contacts": contact_details,
            "_debug": {
                "total_contacts_found": total_contacts,
                "filtered_by_date": filtered_out,
                "no_date_found": no_date,
                "contacts_in_range": len(contact_details),
                "date_range": f"{start_date} to {end_date}"
            }
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
    
    meta_token = decrypt_token(meta_account.access_token_encrypted)
    lucidbot_token = decrypt_token(lucidbot_conn.api_token_encrypted) if lucidbot_conn else None
    
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
        
        # Obtener datos de LucidBot si hay conexión
        if lucidbot_token:
            lucid_data = await get_lucidbot_contacts_by_ad(
                lucidbot_token, 
                ad_id,
                start_date,
                end_date
            )
        else:
            lucid_data = {"leads": 0, "sales": 0, "revenue": 0}
        
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


# ========== DEBUG ENDPOINTS ==========

@router.get("/debug/lucidbot-raw/{ad_id}")
async def debug_lucidbot_raw(
    ad_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    DEBUG: Ver datos crudos de LucidBot para un ad_id específico
    Útil para verificar estructura de datos y campos disponibles
    """
    lucidbot_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not lucidbot_conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conexión de LucidBot no encontrada"
        )
    
    lucidbot_token = decrypt_token(lucidbot_conn.api_token_encrypted)
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{LUCIDBOT_BASE_URL}/users/find_by_custom_field",
            headers={
                "X-ACCESS-TOKEN": lucidbot_token,
                "Accept": "application/json"
            },
            params={
                "field_id": "728462",
                "value": ad_id
            },
            timeout=30
        )
        
        if response.status_code != 200:
            return {
                "error": f"Error {response.status_code}",
                "response": response.text
            }
        
        data = response.json()
        contacts = data.get("data", [])
        
        # Mostrar estructura de primeros 3 contactos
        sample_contacts = []
        for contact in contacts[:3]:
            sample_contacts.append({
                "id": contact.get("id"),
                "full_name": contact.get("full_name"),
                "phone": contact.get("phone"),
                "created_at": contact.get("created_at"),
                "subscribed_date": contact.get("subscribed_date"),
                "timezone": contact.get("timezone"),
                "custom_fields_keys": list(contact.get("custom_fields", {}).keys()),
                "Total_a_pagar": contact.get("custom_fields", {}).get("Total a pagar"),
                "Estado_pedido": contact.get("custom_fields", {}).get("Estado_pedido"),
                "Ciudad": contact.get("custom_fields", {}).get("Ciudad")
            })
        
        return {
            "total_contacts": len(contacts),
            "sample_contacts": sample_contacts,
            "all_custom_field_keys": list(set(
                key 
                for contact in contacts 
                for key in contact.get("custom_fields", {}).keys()
            ))
        }


@router.get("/debug/test-date-filter")
async def debug_test_date_filter(
    ad_id: str,
    start_date: str,
    end_date: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    DEBUG: Probar filtrado de fechas para un ad_id
    Muestra qué contactos pasan el filtro y cuáles no
    """
    lucidbot_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not lucidbot_conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Conexión de LucidBot no encontrada"
        )
    
    lucidbot_token = decrypt_token(lucidbot_conn.api_token_encrypted)
    
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{LUCIDBOT_BASE_URL}/users/find_by_custom_field",
            headers={
                "X-ACCESS-TOKEN": lucidbot_token,
                "Accept": "application/json"
            },
            params={
                "field_id": "728462",
                "value": ad_id
            },
            timeout=30
        )
        
        if response.status_code != 200:
            return {"error": f"Error {response.status_code}"}
        
        contacts = response.json().get("data", [])
        
        passed = []
        filtered_out = []
        
        for contact in contacts:
            created_at = contact.get("created_at", "")
            custom_fields = contact.get("custom_fields", {})
            total_paid = custom_fields.get("Total a pagar")
            
            in_range = is_date_in_range(created_at, start_date, end_date) if created_at else False
            
            contact_summary = {
                "name": contact.get("full_name", ""),
                "created_at": created_at,
                "in_date_range": in_range,
                "has_sale": total_paid is not None,
                "total_paid": total_paid
            }
            
            if in_range:
                passed.append(contact_summary)
            else:
                filtered_out.append(contact_summary)
        
        # Contar ventas en los que pasaron
        sales_count = sum(1 for c in passed if c["has_sale"])
        
        return {
            "filter_params": {
                "ad_id": ad_id,
                "start_date": start_date,
                "end_date": end_date
            },
            "results": {
                "total_contacts": len(contacts),
                "passed_filter": len(passed),
                "filtered_out": len(filtered_out),
                "sales_in_range": sales_count
            },
            "contacts_passed": passed[:20],  # Primeros 20
            "contacts_filtered_out_sample": filtered_out[:10]  # Muestra de filtrados
        }
