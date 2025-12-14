"""
Router de Analytics - FIXED
Combina datos de Meta + LucidBot para calcular CPA, ROAS, etc.

FIX: custom_fields viene como ARRAY, no como diccionario
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import httpx
import re

from database import get_db, User, MetaAccount, LucidbotConnection
from routers.auth import get_current_user
from utils import decrypt_token

router = APIRouter()

META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"
LUCIDBOT_BASE_URL = "https://panel.lucidbot.co/api"

# ========== IDs DE CAMPOS PERSONALIZADOS LUCIDBOT ==========
# Estos IDs son específicos de tu cuenta - ajustar si cambian
LUCIDBOT_FIELD_IDS = {
    "total_a_pagar": 117867,
    "id_pedido": 114210,
    "id_pedido_lucidbot": 95659,
    "fecha_confirmacion_orden": 518707,
    "anuncio_facebook": 728462,
    # Agrega más campos según necesites
}


# ========== HELPERS ==========

def parse_custom_fields_array(custom_fields_array: list) -> Dict[int, Any]:
    """
    Convierte el array de custom_fields de LucidBot a un diccionario por ID
    
    LucidBot devuelve:
    [
        {"id": 117867, "type": 1, "value": "59900"},
        {"id": 728462, "type": 0, "value": "120237294955200647"}
    ]
    
    Esta función retorna:
    {
        117867: "59900",
        728462: "120237294955200647"
    }
    """
    if not custom_fields_array:
        return {}
    
    # Si ya es un diccionario (por si acaso), manejarlo
    if isinstance(custom_fields_array, dict):
        return custom_fields_array
    
    # Si es una lista, convertir a diccionario por ID
    if isinstance(custom_fields_array, list):
        result = {}
        for field in custom_fields_array:
            if isinstance(field, dict) and "id" in field:
                field_id = field.get("id")
                field_value = field.get("value")
                result[field_id] = field_value
        return result
    
    return {}


def get_custom_field_value(custom_fields: Dict[int, Any], field_id: int) -> Optional[str]:
    """
    Obtiene el valor de un campo personalizado por su ID
    """
    return custom_fields.get(field_id)


def contact_is_sale(custom_fields: Dict[int, Any]) -> tuple[bool, float]:
    """
    Determina si un contacto es una venta basándose en el campo "Total a pagar"
    
    Returns:
        tuple: (es_venta: bool, monto: float)
    """
    total_a_pagar_id = LUCIDBOT_FIELD_IDS["total_a_pagar"]
    total_paid = get_custom_field_value(custom_fields, total_a_pagar_id)
    
    if total_paid:
        try:
            amount = float(total_paid)
            if amount > 0:
                return True, amount
        except (ValueError, TypeError):
            pass
    
    return False, 0.0


def parse_lucidbot_date(date_str: str) -> Optional[datetime]:
    """
    Parsear fecha de LucidBot a datetime
    LucidBot usa hora Colombia (UTC-5), no convertimos nada
    
    Formatos observados en LucidBot:
    - "13/12/2025 7:08" (hora sin cero inicial)
    - "13/12/2025 17:08"
    - "13/12/2025"
    """
    if not date_str:
        return None
    
    # Limpiar el string
    date_str = str(date_str).strip()
    
    # Patrón: DD/MM/YYYY H:MM o DD/MM/YYYY HH:MM (con o sin segundos)
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
    
    # Patrón: Solo fecha DD/MM/YYYY
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
    
    # Timestamp Unix (milisegundos o segundos)
    try:
        timestamp = int(date_str)
        # Si es mayor a 10 dígitos, es milisegundos
        if timestamp > 9999999999:
            timestamp = timestamp / 1000
        return datetime.fromtimestamp(timestamp)
    except (ValueError, TypeError, OSError):
        pass
    
    # Formatos ISO estándar como fallback
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ]
    
    for fmt in formats:
        try:
            parsed = datetime.strptime(date_str, fmt)
            return parsed
        except ValueError:
            continue
    
    return None


def is_date_in_range(date_str: str, start_date: str, end_date: str) -> bool:
    """
    Verificar si una fecha está dentro del rango
    Todo en hora Colombia, sin conversiones
    """
    contact_date = parse_lucidbot_date(date_str)
    if not contact_date:
        return False
    
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        
        return start <= contact_date <= end
    except ValueError:
        return False


async def get_meta_ads_with_hierarchy(access_token: str, account_id: str, start_date: str, end_date: str):
    """Obtener métricas de Meta Ads CON jerarquía de campaña/conjunto"""
    async with httpx.AsyncClient(timeout=120) as client:
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


async def get_lucidbot_contacts_by_ad(
    api_token: str, 
    ad_id: str, 
    start_date: str,
    end_date: str,
    ad_field_id: str = "728462"
):
    """
    Obtener contactos de LucidBot por Ad ID FILTRADOS POR FECHA
    
    FIX: Ahora parsea correctamente custom_fields como ARRAY
    """
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
        
        # Contadores para debug
        total_contacts = len(contacts)
        filtered_out = 0
        
        for contact in contacts:
            # ========== FIX: Parsear custom_fields como ARRAY ==========
            custom_fields_raw = contact.get("custom_fields", [])
            custom_fields = parse_custom_fields_array(custom_fields_raw)
            
            # OBTENER FECHA DEL CONTACTO
            contact_date = None
            
            # 1. Intentar fecha de confirmación de orden (más preciso para ventas)
            fecha_confirmacion_id = LUCIDBOT_FIELD_IDS.get("fecha_confirmacion_orden")
            if fecha_confirmacion_id:
                contact_date = get_custom_field_value(custom_fields, fecha_confirmacion_id)
            
            # 2. Si no, usar subscribed_date del contacto
            if not contact_date and contact.get("subscribed_date"):
                contact_date = contact.get("subscribed_date")
            
            # 3. Fallback a otros campos de fecha conocidos
            if not contact_date:
                # Buscar campo "Contact" por ID si lo tienes configurado
                for field_id, value in custom_fields.items():
                    if value and isinstance(value, str) and "/" in value:
                        # Parece una fecha DD/MM/YYYY
                        contact_date = value
                        break
            
            # Si no hay fecha, saltar este contacto
            if not contact_date:
                filtered_out += 1
                continue
            
            # FILTRAR POR FECHA
            if not is_date_in_range(str(contact_date), start_date, end_date):
                filtered_out += 1
                continue
            
            # ========== FIX: Determinar si es venta usando el ID correcto ==========
            is_sale, amount = contact_is_sale(custom_fields)
            
            contact_info = {
                "name": f"{contact.get('first_name', '')} {contact.get('last_name', '')}".strip(),
                "phone": contact.get("phone", ""),
                "created_at": contact_date,
                "id_pedido": get_custom_field_value(custom_fields, LUCIDBOT_FIELD_IDS.get("id_pedido", 0)),
                "id_pedido_lucidbot": get_custom_field_value(custom_fields, LUCIDBOT_FIELD_IDS.get("id_pedido_lucidbot", 0)),
            }
            
            if is_sale:
                sales += 1
                revenue += amount
                contact_info["is_sale"] = True
                contact_info["amount"] = amount
            else:
                leads += 1
                contact_info["is_sale"] = False
            
            contact_details.append(contact_info)
        
        return {
            "leads": leads, 
            "sales": sales, 
            "revenue": revenue,
            "contacts": contact_details,
            "_debug": {
                "total_contacts_found": total_contacts,
                "filtered_by_date": filtered_out,
                "contacts_in_range": len(contact_details)
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
            "conversion_rate": round(conversion_rate, 2),
            # Debug info (quitar en producción si quieres)
            "_lucidbot_debug": lucid_data.get("_debug", {})
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
