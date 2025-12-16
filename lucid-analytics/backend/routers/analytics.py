"""
Router de Analytics
Combina datos de Meta + LucidBot para calcular CPA, ROAS, etc.

CAMBIOS v2.1 (Fix ventas incorrectas):
- Agregada función transform_custom_fields() para convertir array a diccionario
- Agregado endpoint /debug/lucidbot-raw para diagnosticar datos crudos
- Removida compensación de timezone innecesaria (campo Contact ya está en hora local)
- Mejorado logging para debug
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
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

# Ya NO usamos offset porque el campo "Contact" está en hora Colombia
# Solo lo usamos si usamos created_at que viene en UTC
UTC_OFFSET_HOURS = 5


# ========== HELPERS ==========

def transform_custom_fields(custom_fields_data, field_names_map: dict = None) -> dict:
    """
    Transforma custom_fields de array a diccionario.
    
    La API de LucidBot devuelve:
    [{"id": 728462, "type": 0, "value": "120236155688730647"}, ...]
    
    Y lo convertimos a:
    {"728462": "120236155688730647", "Contact": "01/12/2025 12:52", ...}
    
    Args:
        custom_fields_data: Array o dict de custom_fields
        field_names_map: Mapeo opcional de ID a nombre (ej: {"728462": "Anuncio Facebook"})
    """
    # Si ya es diccionario, devolverlo tal cual
    if isinstance(custom_fields_data, dict):
        return custom_fields_data
    
    # Si es None o vacío
    if not custom_fields_data:
        return {}
    
    # Si es array, transformar
    if isinstance(custom_fields_data, list):
        result = {}
        for field in custom_fields_data:
            if isinstance(field, dict):
                field_id = str(field.get("id", ""))
                field_value = field.get("value", "")
                field_name = field.get("name", "")  # Algunos endpoints incluyen el nombre
                
                # Guardar por ID
                if field_id:
                    result[field_id] = field_value
                
                # También guardar por nombre si está disponible
                if field_name:
                    result[field_name] = field_value
                    
                # Usar mapeo de nombres si se proporciona
                if field_names_map and field_id in field_names_map:
                    result[field_names_map[field_id]] = field_value
        
        return result
    
    # Tipo desconocido
    return {}


def parse_lucidbot_date(date_str: str) -> Optional[datetime]:
    """
    Parsear fecha de LucidBot a datetime
    
    Formatos observados en LucidBot:
    - "2025-12-13 21:21:03" (formato created_at de API - UTC)
    - "13/12/2025 7:08" (hora sin cero inicial - hora local Colombia)
    - "13/12/2025 17:08" (hora local Colombia)
    - "01/12/2025 12:52" (hora local Colombia)
    - "13/12/2025"
    """
    if not date_str:
        return None
    
    # Limpiar el string
    date_str = str(date_str).strip()
    
    # Formatos ISO y estándar primero (más comunes en API - estos son UTC)
    formats = [
        "%Y-%m-%d %H:%M:%S",       # "2025-12-13 21:21:03" - formato de created_at
        "%Y-%m-%d %H:%M:%S.%f",    # Con microsegundos
        "%Y-%m-%dT%H:%M:%S.%fZ",   # ISO con microsegundos UTC
        "%Y-%m-%dT%H:%M:%SZ",      # ISO sin microsegundos UTC
        "%Y-%m-%dT%H:%M:%S.%f",    # ISO con microsegundos sin Z
        "%Y-%m-%dT%H:%M:%S",       # ISO sin Z
        "%Y-%m-%d",                # Solo fecha ISO
    ]
    
    for fmt in formats:
        try:
            parsed = datetime.strptime(date_str, fmt)
            return parsed
        except ValueError:
            continue
    
    # Parseo manual para formato DD/MM/YYYY H:MM o DD/MM/YYYY HH:MM
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
    
    return None


def is_date_in_range(date_str: str, start_date: str, end_date: str, is_utc: bool = False) -> bool:
    """
    Verificar si una fecha está dentro del rango
    
    Args:
        date_str: Fecha del contacto (ej: "01/12/2025 12:52" o "2025-12-01 17:52:00")
        start_date: Fecha inicio YYYY-MM-DD (ej: "2025-12-01")
        end_date: Fecha fin YYYY-MM-DD (ej: "2025-12-01")
        is_utc: Si True, la fecha viene en UTC y hay que compensar timezone
    """
    contact_date = parse_lucidbot_date(date_str)
    if not contact_date:
        print(f"[DATE] No se pudo parsear fecha: '{date_str}'")
        return False
    
    try:
        # Rango base en hora local
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        
        # Si la fecha viene en UTC, necesitamos ajustar el rango
        # Colombia = UTC-5, entonces para buscar 1/dic Colombia en datos UTC:
        # - Inicio: 1/dic 00:00 Colombia = 1/dic 05:00 UTC
        # - Fin: 1/dic 23:59 Colombia = 2/dic 04:59 UTC
        if is_utc:
            start = start + timedelta(hours=UTC_OFFSET_HOURS)
            end = end + timedelta(hours=UTC_OFFSET_HOURS)
        
        in_range = start <= contact_date <= end
        
        return in_range
    except ValueError as e:
        print(f"[DATE] Error parseando rango: {e}")
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


async def get_lucidbot_contacts_by_ad(
    api_token: str, 
    ad_id: str, 
    start_date: str,
    end_date: str,
    ad_field_id: str = "728462"
):
    """
    Obtener contactos de LucidBot por Ad ID FILTRADOS POR FECHA
    
    CAMBIOS v2.1:
    - Transformación de custom_fields de array a diccionario
    - Mejor manejo de fechas (detecta si es UTC o local)
    - Logging mejorado para debug
    
    Args:
        api_token: Token de API de LucidBot
        ad_id: ID del anuncio de Facebook
        start_date: Fecha inicio (YYYY-MM-DD) en hora Colombia
        end_date: Fecha fin (YYYY-MM-DD) en hora Colombia
        ad_field_id: ID del campo personalizado donde está el Ad ID
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
            print(f"[LUCIDBOT] Error API: status={response.status_code}, ad_id={ad_id}")
            return {"leads": 0, "sales": 0, "revenue": 0, "contacts": []}
        
        contacts = response.json().get("data", [])
        
        leads = 0
        sales = 0
        revenue = 0
        contact_details = []
        
        # Contadores para debug
        total_contacts = len(contacts)
        filtered_out = 0
        no_date = 0
        
        print(f"[LUCIDBOT] ad_id={ad_id}: encontrados {total_contacts} contactos, filtrando {start_date} a {end_date}")
        
        for contact in contacts:
            # ========== FIX: Transformar custom_fields de array a dict ==========
            raw_custom_fields = contact.get("custom_fields", {})
            custom_fields = transform_custom_fields(raw_custom_fields)
            
            # OBTENER FECHA DEL CONTACTO
            # Prioridad: custom_field "Contact" (hora local) > created_at (UTC)
            contact_date = None
            is_utc = False
            
            # 1. Primero buscar en custom_fields "Contact" (ya está en hora Colombia)
            # Probar varios nombres posibles del campo
            for field_name in ["Contact", "contact", "Fecha", "fecha", "Fecha_Creacion", "created"]:
                if custom_fields.get(field_name):
                    contact_date = custom_fields.get(field_name)
                    is_utc = False  # Campo Contact está en hora local
                    break
            
            # 2. Si no hay campo Contact, usar created_at del API (viene en UTC)
            if not contact_date and contact.get("created_at"):
                contact_date = contact.get("created_at")
                is_utc = True  # created_at viene en UTC
            
            # Si no hay fecha, saltar este contacto
            if not contact_date:
                no_date += 1
                filtered_out += 1
                print(f"[LUCIDBOT] Contacto sin fecha: {contact.get('full_name', 'Sin nombre')} - custom_fields keys: {list(custom_fields.keys())}")
                continue
            
            # FILTRAR POR FECHA
            if not is_date_in_range(str(contact_date), start_date, end_date, is_utc=is_utc):
                filtered_out += 1
                continue
            
            # Verificar si es venta o lead
            total_paid = custom_fields.get("Total a pagar") or custom_fields.get("total_a_pagar")
            
            contact_info = {
                "name": contact.get("full_name", ""),
                "phone": contact.get("phone", ""),
                "created_at": contact_date,
                "is_utc": is_utc,
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
        
        print(f"[LUCIDBOT] ad_id={ad_id}: {sales} ventas, {leads} leads, filtrados={filtered_out} (sin_fecha={no_date})")
        
        return {
            "leads": leads, 
            "sales": sales, 
            "revenue": revenue,
            "contacts": contact_details,
            "_debug": {
                "total_contacts_found": total_contacts,
                "filtered_by_date": filtered_out,
                "no_date_field": no_date,
                "contacts_in_range": len(contact_details),
                "utc_offset_hours": UTC_OFFSET_HOURS
            }
        }


# ========== ENDPOINTS ==========

@router.get("/debug/lucidbot-raw/{ad_id}")
async def debug_lucidbot_raw(
    ad_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    ENDPOINT DE DEBUG: Ver datos CRUDOS de LucidBot para un ad_id
    
    Útil para diagnosticar problemas de:
    - Formato de custom_fields (array vs dict)
    - Nombres de campos
    - Formato de fechas
    """
    lucidbot_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not lucidbot_conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No hay conexión de LucidBot configurada"
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
                "field_id": "728462",  # Anuncio Facebook
                "value": ad_id
            },
            timeout=30
        )
        
        if response.status_code != 200:
            return {
                "error": f"LucidBot API error: {response.status_code}",
                "response": response.text
            }
        
        raw_data = response.json()
        contacts = raw_data.get("data", [])
        
        # Analizar estructura de cada contacto
        analysis = []
        for i, contact in enumerate(contacts[:5]):  # Solo primeros 5 para no saturar
            raw_cf = contact.get("custom_fields", {})
            transformed_cf = transform_custom_fields(raw_cf)
            
            analysis.append({
                "index": i,
                "full_name": contact.get("full_name", ""),
                "phone": contact.get("phone", ""),
                "created_at": contact.get("created_at", ""),
                "custom_fields_type": type(raw_cf).__name__,
                "custom_fields_raw_sample": raw_cf[:3] if isinstance(raw_cf, list) else dict(list(raw_cf.items())[:3]) if isinstance(raw_cf, dict) else str(raw_cf),
                "custom_fields_transformed": transformed_cf,
                "has_contact_field": "Contact" in transformed_cf or "contact" in transformed_cf,
                "has_total_a_pagar": "Total a pagar" in transformed_cf or "total_a_pagar" in transformed_cf
            })
        
        return {
            "ad_id": ad_id,
            "total_contacts": len(contacts),
            "api_response_keys": list(raw_data.keys()),
            "contacts_analysis": analysis,
            "tip": "Revisa 'custom_fields_type' - si es 'list', el fix es necesario"
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
    ENDPOINT DE DEBUG: Probar filtro de fechas con logging detallado
    """
    lucidbot_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not lucidbot_conn:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No hay conexión de LucidBot configurada"
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
            return {"error": f"API error: {response.status_code}"}
        
        contacts = response.json().get("data", [])
        
        results = []
        for contact in contacts:
            raw_cf = contact.get("custom_fields", {})
            custom_fields = transform_custom_fields(raw_cf)
            
            # Buscar fecha
            contact_date = None
            is_utc = False
            date_source = None
            
            for field_name in ["Contact", "contact", "Fecha", "fecha"]:
                if custom_fields.get(field_name):
                    contact_date = custom_fields.get(field_name)
                    date_source = f"custom_field:{field_name}"
                    is_utc = False
                    break
            
            if not contact_date and contact.get("created_at"):
                contact_date = contact.get("created_at")
                date_source = "created_at"
                is_utc = True
            
            # Probar parseo
            parsed = parse_lucidbot_date(str(contact_date)) if contact_date else None
            in_range = is_date_in_range(str(contact_date), start_date, end_date, is_utc=is_utc) if contact_date else False
            
            results.append({
                "name": contact.get("full_name", ""),
                "phone": contact.get("phone", ""),
                "date_source": date_source,
                "raw_date": contact_date,
                "parsed_date": str(parsed) if parsed else None,
                "is_utc": is_utc,
                "in_range": in_range,
                "has_total_a_pagar": bool(custom_fields.get("Total a pagar"))
            })
        
        return {
            "ad_id": ad_id,
            "date_range": {"start": start_date, "end": end_date},
            "total_contacts": len(contacts),
            "contacts_in_range": sum(1 for r in results if r["in_range"]),
            "contacts": results
        }


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
