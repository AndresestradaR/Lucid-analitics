"""
Router de Analytics
Combina datos de Meta + LucidBot para calcular CPA, ROAS, etc.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime, timedelta
import httpx
import re
import sys
import asyncio

from database import get_db, User, MetaAccount, LucidbotConnection
from routers.auth import get_current_user
from utils import decrypt_token

router = APIRouter()

META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"
LUCIDBOT_BASE_URL = "https://panel.lucidbot.co/api"

# Offset UTC a Colombia
# Colombia es UTC-5, entonces para convertir hora Colombia a UTC sumamos 5
# Si usuario pide 13/dic Colombia, en UTC es desde 13/dic 05:00 hasta 14/dic 04:59
UTC_OFFSET_HOURS = 5


# ========== HELPERS ==========

def parse_lucidbot_date(date_str: str) -> Optional[datetime]:
    """
    Parsear fecha de LucidBot a datetime
    
    Formatos observados en LucidBot:
    - "2025-12-13 21:21:03" (formato created_at de API)
    - "13/12/2025 7:08" (hora sin cero inicial)
    - "13/12/2025 17:08"
    - "13/12/2025"
    """
    if not date_str:
        return None
    
    # Limpiar el string
    date_str = str(date_str).strip()
    
    # Formatos ISO y estándar primero (más comunes en API)
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


def is_date_in_range(date_str: str, start_date: str, end_date: str) -> bool:
    """
    Verificar si una fecha está dentro del rango.

    IMPORTANTE: LucidBot devuelve fechas en UTC, pero el usuario consulta por día en
    hora Colombia (UTC-5). Convertimos el rango de días en Colombia a un rango UTC
    equivalente, usando FIN EXCLUSIVO para evitar errores en bordes.

    Args:
        date_str: Fecha del contacto (ej: "2025-12-13 21:21:03")
        start_date: Fecha inicio YYYY-MM-DD (ej: "2025-12-13")
        end_date: Fecha fin YYYY-MM-DD (ej: "2025-12-15")
    """
    contact_date = parse_lucidbot_date(date_str)
    if not contact_date:
        return False

    try:
        # Colombia = UTC-5, entonces Colombia 00:00 = UTC 05:00
        start = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(hours=UTC_OFFSET_HOURS)

        # FIN EXCLUSIVO: el fin es el inicio del día siguiente (en Colombia) convertido a UTC
        end_exclusive = (datetime.strptime(end_date, "%Y-%m-%d") + timedelta(days=1)) + timedelta(hours=UTC_OFFSET_HOURS)

        return start <= contact_date < end_exclusive
    except ValueError:
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
        
        # Paso 3: Agregar por ad_id para evitar duplicados (si Meta devuelve múltiples filas)
        aggregated = {}
        
        for insight in insights_data:
            ad_id = insight.get("ad_id")
            if not ad_id:
                continue
            
            if ad_id not in aggregated:
                aggregated[ad_id] = {
                    "spend": 0.0,
                    "impressions": 0,
                    "clicks": 0,
                    "reach": 0,
                    "messaging_conversations": 0,
                    "cost_per_messaging": 0.0,
                }
            
            aggregated[ad_id]["spend"] += float(insight.get("spend", 0) or 0)
            aggregated[ad_id]["impressions"] += int(insight.get("impressions", 0) or 0)
            aggregated[ad_id]["clicks"] += int(insight.get("clicks", 0) or 0)
            aggregated[ad_id]["reach"] += int(insight.get("reach", 0) or 0)
            
            # Actions: sumar conversaciones de mensajería
            actions = insight.get("actions", [])
            for action in actions:
                action_type = action.get("action_type", "")
                if "messaging" in action_type.lower() or "conversation" in action_type.lower():
                    aggregated[ad_id]["messaging_conversations"] += int(action.get("value", 0) or 0)
            
            # cost_per_action_type: tomar el último valor disponible para messaging
            cost_per_actions = insight.get("cost_per_action_type", [])
            for cpa in cost_per_actions:
                action_type = cpa.get("action_type", "")
                if "messaging" in action_type.lower() or "conversation" in action_type.lower():
                    try:
                        aggregated[ad_id]["cost_per_messaging"] = float(cpa.get("value", 0) or 0)
                    except (TypeError, ValueError):
                        pass
                    break
        
        # Paso 4: Construir resultado final
        result = []
        for ad_id, agg in aggregated.items():
            ad_info = ads_info.get(ad_id, {})
            spend = agg["spend"]
            impressions = agg["impressions"]
            clicks = agg["clicks"]
            
            # Calcular métricas derivadas
            ctr = (clicks / impressions * 100) if impressions else 0
            cpm = (spend / impressions * 1000) if impressions else 0
            cpc = (spend / clicks) if clicks else 0
            
            result.append({
                "ad_id": ad_id,
                "ad_name": ad_info.get("ad_name", ""),
                "status": ad_info.get("status", ""),
                "campaign_id": ad_info.get("campaign_id", ""),
                "campaign_name": ad_info.get("campaign_name", ""),
                "adset_id": ad_info.get("adset_id", ""),
                "adset_name": ad_info.get("adset_name", ""),
                "daily_budget": ad_info.get("daily_budget"),
                "lifetime_budget": ad_info.get("lifetime_budget"),
                "spend": spend,
                "impressions": impressions,
                "clicks": clicks,
                "ctr": ctr,
                "cpm": cpm,
                "cpc": cpc,
                "reach": agg["reach"],
                "messaging_conversations": agg["messaging_conversations"],
                "cost_per_messaging": agg["cost_per_messaging"]
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
    CON PAGINACIÓN para obtener más de 100 contactos
    
    Args:
        api_token: Token de API de LucidBot
        ad_id: ID del anuncio de Facebook
        start_date: Fecha inicio (YYYY-MM-DD) en hora Colombia
        end_date: Fecha fin (YYYY-MM-DD) en hora Colombia
        ad_field_id: ID del campo personalizado donde está el Ad ID
    """
    # PAGINACIÓN: máximo 10 páginas = 1000 contactos
    all_contacts = []
    page = 1
    max_pages = 10
    
    async with httpx.AsyncClient(timeout=60.0) as client:  # Timeout aumentado a 60s
        while page <= max_pages:
            response = await client.get(
                f"{LUCIDBOT_BASE_URL}/users/find_by_custom_field",
                headers={
                    "X-ACCESS-TOKEN": api_token,
                    "Accept": "application/json"
                },
                params={
                    "field_id": ad_field_id,
                    "value": ad_id,
                    "page": page
                },
                timeout=30
            )
            
            if response.status_code != 200:
                print(f"[LUCIDBOT] ad={ad_id} page={page} ERROR={response.status_code}")
                break
            
            page_contacts = response.json().get("data", [])
            
            if not page_contacts:
                break
            
            all_contacts.extend(page_contacts)
            
            # Si obtuvo menos de 100, ya no hay más páginas
            if len(page_contacts) < 100:
                break
            
            page += 1
    
    if not all_contacts:
        return {"leads": 0, "sales": 0, "revenue": 0, "contacts": []}
    
    contacts = all_contacts
    
    leads = 0
    sales = 0
    revenue = 0
    contact_details = []
    
    # Contadores para debug
    total_contacts = len(contacts)
    filtered_out = 0
    
    for contact in contacts:
        # OBTENER FECHA DEL CONTACTO
        # Prioridad: created_at del API > custom_field "Contact" > otros
        custom_fields = contact.get("custom_fields", {})
        
        # Buscar la fecha en diferentes lugares
        contact_date = None
        
        # 1. Primero intentar created_at del contacto
        if contact.get("created_at"):
            contact_date = contact.get("created_at")
        
        # 2. Si no, buscar en custom_fields "Contact" (como se ve en LucidBot)
        if not contact_date and custom_fields.get("Contact"):
            contact_date = custom_fields.get("Contact")
        
        # 3. Otras alternativas
        if not contact_date:
            contact_date = custom_fields.get("Fecha", "") or custom_fields.get("fecha", "") or custom_fields.get("Fecha_Creacion", "")
        
        # Si no hay fecha, saltar este contacto
        if not contact_date:
            filtered_out += 1
            continue
        
        # FILTRAR POR FECHA - ahora con compensación de timezone
        if not is_date_in_range(str(contact_date), start_date, end_date):
            filtered_out += 1
            continue
        
        total_paid = custom_fields.get("Total a pagar")
        
        contact_info = {
            "name": contact.get("full_name", ""),
            "phone": contact.get("phone", ""),
            "created_at": contact_date,
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
    
    # Log de resultado por anuncio
    print(f"[LUCIDBOT] ad={ad_id}: {total_contacts} contactos -> {sales} ventas, {leads} leads (filtrados={filtered_out})")
    sys.stdout.flush()
    
    return {
        "leads": leads, 
        "sales": sales, 
        "revenue": revenue,
        "contacts": contact_details,
        "_debug": {
            "total_contacts_found": total_contacts,
            "filtered_by_date": filtered_out,
            "contacts_in_range": len(contact_details),
            "utc_offset_hours": UTC_OFFSET_HOURS
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
    
    print(f"[DASHBOARD] account={account_id}, dates={start_date} to {end_date}, lucidbot={'SI' if lucidbot_token else 'NO'}")
    
    # Obtener anuncios de Meta CON jerarquía
    meta_ads = await get_meta_ads_with_hierarchy(meta_token, account_id, start_date, end_date)
    
    print(f"[DASHBOARD] Meta ads encontrados: {len(meta_ads) if meta_ads else 0}")
    
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
    
    # CONSULTAS EN PARALELO para velocidad (máx 5 simultáneas)
    if lucidbot_token:
        semaphore = asyncio.Semaphore(5)  # Máximo 5 consultas simultáneas
        
        async def fetch_lucidbot_data(ad):
            ad_id = ad.get("ad_id")
            async with semaphore:
                try:
                    return ad_id, await get_lucidbot_contacts_by_ad(
                        lucidbot_token, 
                        ad_id,
                        start_date,
                        end_date
                    )
                except Exception as e:
                    print(f"[LUCIDBOT] ERROR ad={ad_id}: {str(e)}")
                    sys.stdout.flush()
                    return ad_id, {"leads": 0, "sales": 0, "revenue": 0}
        
        # Ejecutar todas las consultas en paralelo
        print(f"[DASHBOARD] Consultando LucidBot para {len(meta_ads)} anuncios en paralelo...")
        sys.stdout.flush()
        
        results = await asyncio.gather(*[fetch_lucidbot_data(ad) for ad in meta_ads])
        lucidbot_data = {ad_id: data for ad_id, data in results}
        
        print(f"[DASHBOARD] LucidBot completado")
        sys.stdout.flush()
    else:
        lucidbot_data = {}
    
    for ad in meta_ads:
        ad_id = ad.get("ad_id")
        spend = float(ad.get("spend", 0))
        
        # Obtener datos de LucidBot del resultado paralelo
        lucid_data = lucidbot_data.get(ad_id, {"leads": 0, "sales": 0, "revenue": 0})
        
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
    
    # LOG FINAL - TOTALES
    print(f"[DASHBOARD] ========== RESUMEN FINAL ==========")
    print(f"[DASHBOARD] Total Leads: {total_leads}")
    print(f"[DASHBOARD] Total Ventas: {total_sales}")
    print(f"[DASHBOARD] Total Revenue: ${total_revenue:,.0f}")
    print(f"[DASHBOARD] Total Spend: ${total_spend:,.0f}")
    print(f"[DASHBOARD] CPA: ${avg_cpa:,.0f}")
    print(f"[DASHBOARD] ROAS: {avg_roas:.2f}x")
    print(f"[DASHBOARD] =====================================")
    sys.stdout.flush()  # Forzar que los logs se muestren
    
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
