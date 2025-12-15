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
    
    SIMPLIFICADO: Comparamos solo las fechas (YYYY-MM-DD) sin compensación UTC.
    Si el contacto fue creado el 2025-12-05, y el rango es 2025-12-01 a 2025-12-07,
    entonces está en rango.
    """
    contact_date = parse_lucidbot_date(date_str)
    if not contact_date:
        return False

    try:
        # Comparar solo la parte de fecha (ignorar hora)
        contact_date_only = contact_date.date()
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        return start <= contact_date_only <= end
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
        
        # DEBUG: Mostrar primer contacto completo
        if contacts.index(contact) == 0:
            print(f"[DEBUG] Primer contacto custom_fields keys: {list(custom_fields.keys())}")
            print(f"[DEBUG] created_at: {contact.get('created_at')}")
            print(f"[DEBUG] Total a pagar: {custom_fields.get('Total a pagar')}")
            sys.stdout.flush()
        
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
            if filtered_out <= 2:
                print(f"[DEBUG] Sin fecha - created_at ausente, keys={list(custom_fields.keys())[:5]}")
                sys.stdout.flush()
            continue
        
        # FILTRAR POR FECHA - ahora con compensación de timezone
        if not is_date_in_range(str(contact_date), start_date, end_date):
            filtered_out += 1
            if filtered_out <= 5:
                print(f"[DEBUG FILTRADO] fecha={contact_date} NO está en {start_date} a {end_date}")
                sys.stdout.flush()
            continue
        
        total_paid = custom_fields.get("Total a pagar")
        
        # DEBUG: mostrar qué campos tienen los primeros contactos que pasan el filtro
        if len(contact_details) < 3:
            print(f"[DEBUG VENTA] Total a pagar={total_paid}, Calificacion={custom_fields.get('Calificacion_LucidSales', 'N/A')}")
            sys.stdout.flush()
        
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
    if meta_ads:
        ad_ids_meta = [ad.get("ad_id") for ad in meta_ads]
        print(f"[DASHBOARD] Ad IDs de Meta: {ad_ids_meta}")
        sys.stdout.flush()
    
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


@router.get("/debug/lucidbot")
async def debug_lucidbot(
    ad_id: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    ENDPOINT DE DIAGNÓSTICO - Ver datos crudos de LucidBot
    
    Si ad_id está presente: muestra contactos de ese anuncio
    Si no: muestra los últimos 50 contactos con sus Ad IDs
    """
    
    lucidbot_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not lucidbot_conn:
        return {"error": "LucidBot no conectado"}
    
    api_token = decrypt_token(lucidbot_conn.api_token_encrypted)
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        if ad_id:
            # Buscar por Ad ID específico
            response = await client.get(
                f"{LUCIDBOT_BASE_URL}/users/find_by_custom_field",
                headers={"X-ACCESS-TOKEN": api_token, "Accept": "application/json"},
                params={"field_id": "728462", "value": ad_id}
            )
        else:
            # Obtener últimos contactos
            response = await client.get(
                f"{LUCIDBOT_BASE_URL}/users",
                headers={"X-ACCESS-TOKEN": api_token, "Accept": "application/json"},
                params={"limit": 50}
            )
        
        if response.status_code != 200:
            return {"error": f"Error LucidBot: {response.status_code}", "body": response.text[:500]}
        
        contacts = response.json().get("data", [])
        
        # Analizar contactos
        analysis = {
            "total_contacts": len(contacts),
            "with_total_pagar": 0,
            "without_total_pagar": 0,
            "ad_ids_found": set(),
            "dates_found": [],
            "sample_contacts": []
        }
        
        for i, contact in enumerate(contacts[:20]):  # Solo primeros 20 para muestra
            custom_fields = contact.get("custom_fields", {})
            
            # Buscar Ad ID en custom fields
            ad_id_value = None
            for key, value in custom_fields.items():
                if "ad" in key.lower() or "anuncio" in key.lower() or key == "728462":
                    ad_id_value = value
                    if value:
                        analysis["ad_ids_found"].add(str(value))
            
            # Verificar Total a pagar
            total_pagar = custom_fields.get("Total a pagar")
            if total_pagar:
                analysis["with_total_pagar"] += 1
            else:
                analysis["without_total_pagar"] += 1
            
            # Fecha
            created_at = contact.get("created_at", "")
            contact_date = custom_fields.get("Contact", "")
            
            analysis["sample_contacts"].append({
                "id": contact.get("id"),
                "name": contact.get("full_name", "")[:30],
                "created_at": created_at,
                "contact_field": contact_date,
                "ad_id": ad_id_value,
                "total_pagar": total_pagar,
                "calificacion": custom_fields.get("Calificacion_LucidSales", ""),
                "all_custom_fields": list(custom_fields.keys())
            })
        
        # Contar todos
        for contact in contacts:
            custom_fields = contact.get("custom_fields", {})
            if custom_fields.get("Total a pagar"):
                pass  # Ya contamos arriba para los primeros 20
        
        analysis["ad_ids_found"] = list(analysis["ad_ids_found"])
        
        return {
            "query": {"ad_id": ad_id} if ad_id else "últimos 50 contactos",
            "analysis": analysis
        }


@router.get("/diagnostico/paginacion/{ad_id}")
async def diagnostico_paginacion(
    ad_id: str,
    user_id: int,
    db: Session = Depends(get_db)
):
    """
    Prueba diferentes parámetros de paginación para ver cuál funciona
    """
    lucidbot_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == user_id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not lucidbot_conn:
        return {"error": f"No hay conexión LucidBot para user_id={user_id}"}
    
    api_token = decrypt_token(lucidbot_conn.api_token_encrypted)
    
    resultados = {}
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        # Test 1: /users/find_by_custom_field (lo que usamos ahora)
        resp = await client.get(
            f"{LUCIDBOT_BASE_URL}/users/find_by_custom_field",
            headers={"X-ACCESS-TOKEN": api_token, "Accept": "application/json"},
            params={"field_id": "728462", "value": ad_id}
        )
        data = resp.json() if resp.status_code == 200 else {}
        resultados["users_endpoint"] = {
            "count": len(data.get("data", [])),
            "keys": list(data.keys()) if isinstance(data, dict) else None
        }
        
        # Test 2: /contacts/find_by_custom_field (del Swagger)
        resp = await client.get(
            f"{LUCIDBOT_BASE_URL}/contacts/find_by_custom_field",
            headers={"X-ACCESS-TOKEN": api_token, "Accept": "application/json"},
            params={"field_id": "728462", "value": ad_id}
        )
        data = resp.json() if resp.status_code == 200 else {"error": resp.text[:200]}
        resultados["contacts_endpoint"] = {
            "status": resp.status_code,
            "count": len(data.get("data", [])) if isinstance(data, dict) and "data" in data else 0,
            "keys": list(data.keys()) if isinstance(data, dict) else None,
            "error": data.get("error") if isinstance(data, dict) else None
        }
        
        # Test 3: /contacts/find_by_custom_field con limit
        resp = await client.get(
            f"{LUCIDBOT_BASE_URL}/contacts/find_by_custom_field",
            headers={"X-ACCESS-TOKEN": api_token, "Accept": "application/json"},
            params={"field_id": "728462", "value": ad_id, "limit": 500}
        )
        data = resp.json() if resp.status_code == 200 else {}
        resultados["contacts_limit_500"] = {
            "status": resp.status_code,
            "count": len(data.get("data", [])) if isinstance(data, dict) and "data" in data else 0
        }
        
        # Test 4: Probar diferentes bases de URL
        for base in ["https://panel.lucidbot.co/api", "https://api.lucidbot.co", "https://panel.lucidbot.co/php"]:
            try:
                resp = await client.get(
                    f"{base}/contacts/find_by_custom_field",
                    headers={"X-ACCESS-TOKEN": api_token, "Accept": "application/json"},
                    params={"field_id": "728462", "value": ad_id},
                    timeout=10
                )
                resultados[f"base_{base.split('//')[-1].replace('/', '_')}"] = {
                    "status": resp.status_code,
                    "count": len(resp.json().get("data", [])) if resp.status_code == 200 else 0
                }
            except Exception as e:
                resultados[f"base_{base.split('//')[-1].replace('/', '_')}"] = {"error": str(e)[:100]}
        
        # Test 5: Verificar metadatos de paginación en respuesta completa
        resp = await client.get(
            f"{LUCIDBOT_BASE_URL}/users/find_by_custom_field",
            headers={"X-ACCESS-TOKEN": api_token, "Accept": "application/json"},
            params={"field_id": "728462", "value": ad_id}
        )
        if resp.status_code == 200:
            full = resp.json()
            resultados["metadata_completo"] = {k: v for k, v in full.items() if k != "data"}
            if not resultados["metadata_completo"]:
                resultados["metadata_completo"] = "No hay metadatos, solo 'data'"
    
    return {
        "ad_id": ad_id,
        "resultados": resultados
    }


@router.get("/diagnostico/fields/list")
async def diagnostico_fields(
    user_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Lista todos los custom fields de LucidBot para encontrar el field_id correcto
    
    Parámetro opcional: ?user_id=4 para probar un usuario específico
    """
    if user_id:
        lucidbot_conn = db.query(LucidbotConnection).filter(
            LucidbotConnection.user_id == user_id,
            LucidbotConnection.is_active == True
        ).first()
    else:
        lucidbot_conn = db.query(LucidbotConnection).filter(
            LucidbotConnection.is_active == True
        ).first()
    
    if not lucidbot_conn:
        return {"error": "LucidBot no conectado"}
    
    api_token = decrypt_token(lucidbot_conn.api_token_encrypted)
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        # Obtener custom fields
        response = await client.get(
            f"{LUCIDBOT_BASE_URL}/accounts/custom_fields",
            headers={"X-ACCESS-TOKEN": api_token, "Accept": "application/json"}
        )
        
        if response.status_code != 200:
            return {"error": f"Error: {response.status_code}", "body": response.text[:500]}
        
        fields = response.json()
        
        # Buscar campos relacionados con "ad" o "anuncio" o "facebook"
        relevant_fields = []
        all_fields = []
        
        for field in fields.get("data", fields) if isinstance(fields, dict) else fields:
            field_info = {
                "id": field.get("id"),
                "name": field.get("name"),
                "type": field.get("type")
            }
            all_fields.append(field_info)
            
            name_lower = (field.get("name") or "").lower()
            if any(x in name_lower for x in ["ad", "anuncio", "facebook", "meta", "campaña", "campaign"]):
                relevant_fields.append(field_info)
        
        return {
            "total_fields": len(all_fields),
            "campos_relevantes_para_ads": relevant_fields,
            "todos_los_campos": all_fields
        }


@router.get("/diagnostico/contactos/muestra")
async def diagnostico_contactos_muestra(
    user_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Obtiene una muestra de contactos recientes para ver qué campos tienen
    
    Parámetro opcional: ?user_id=4 para probar un usuario específico
    """
    if user_id:
        lucidbot_conn = db.query(LucidbotConnection).filter(
            LucidbotConnection.user_id == user_id,
            LucidbotConnection.is_active == True
        ).first()
    else:
        lucidbot_conn = db.query(LucidbotConnection).filter(
            LucidbotConnection.is_active == True
        ).first()
    
    if not lucidbot_conn:
        return {"error": "LucidBot no conectado"}
    
    api_token = decrypt_token(lucidbot_conn.api_token_encrypted)
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(
            f"{LUCIDBOT_BASE_URL}/users",
            headers={"X-ACCESS-TOKEN": api_token, "Accept": "application/json"},
            params={"limit": 20}
        )
        
        if response.status_code != 200:
            return {"error": f"Error: {response.status_code}"}
        
        contacts = response.json().get("data", [])
        
        muestra = []
        ad_ids_encontrados = set()
        campos_con_numeros_largos = {}
        
        for contact in contacts:
            custom_fields = contact.get("custom_fields", {})
            
            contact_info = {
                "id": contact.get("id"),
                "nombre": contact.get("full_name", "")[:30],
                "created_at": contact.get("created_at"),
                "custom_fields_keys": list(custom_fields.keys()),
                "campos_con_valores": {}
            }
            
            # Buscar campos que parezcan Ad IDs (números largos)
            for key, value in custom_fields.items():
                if value:
                    contact_info["campos_con_valores"][key] = str(value)[:50]
                    
                    # Si parece un Ad ID (número largo)
                    str_value = str(value)
                    if str_value.isdigit() and len(str_value) > 10:
                        ad_ids_encontrados.add(str_value)
                        if key not in campos_con_numeros_largos:
                            campos_con_numeros_largos[key] = []
                        if str_value not in campos_con_numeros_largos[key]:
                            campos_con_numeros_largos[key].append(str_value)
            
            muestra.append(contact_info)
        
        return {
            "total_contactos_muestra": len(muestra),
            "ad_ids_encontrados": list(ad_ids_encontrados)[:20],
            "campos_que_contienen_numeros_largos": campos_con_numeros_largos,
            "muestra_contactos": muestra[:5]
        }


@router.get("/diagnostico/cuenta")
async def diagnostico_cuenta(
    user_id: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Verifica la cuenta de LucidBot conectada y prueba diferentes endpoints
    
    Parámetro opcional: ?user_id=4 para probar un usuario específico
    """
    # Buscar TODAS las conexiones de LucidBot
    todas_conexiones = db.query(LucidbotConnection).all()
    
    conexiones_info = []
    for conn in todas_conexiones:
        conexiones_info.append({
            "id": conn.id,
            "user_id": conn.user_id,
            "account_id": conn.account_id,
            "is_active": conn.is_active,
            "created_at": str(conn.created_at) if conn.created_at else None
        })
    
    # Usar conexión específica o la primera activa
    if user_id:
        lucidbot_conn = db.query(LucidbotConnection).filter(
            LucidbotConnection.user_id == user_id,
            LucidbotConnection.is_active == True
        ).first()
    else:
        lucidbot_conn = db.query(LucidbotConnection).filter(
            LucidbotConnection.is_active == True
        ).first()


@router.get("/diagnostico/ad/{ad_id}")
async def diagnostico_ad_completo(
    ad_id: str,
    user_id: int,
    start_date: str = "2025-12-01",
    end_date: str = "2025-12-07",
    db: Session = Depends(get_db)
):
    """
    DIAGNÓSTICO COMPLETO PARA UN AD ID
    
    Uso: /api/analytics/diagnostico/ad/120236155688730647?user_id=4&start_date=2025-12-01&end_date=2025-12-07
    
    - Obtiene TODOS los contactos con paginación
    - Analiza fechas
    - Cuenta ventas por día
    """
    lucidbot_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == user_id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not lucidbot_conn:
        return {"error": f"No hay conexión LucidBot para user_id={user_id}"}
    
    api_token = decrypt_token(lucidbot_conn.api_token_encrypted)
    
    # Obtener contactos - solo 2 páginas porque la API repite datos
    all_contacts = []
    page = 1
    max_pages = 2  # La API repite contactos, no necesitamos más
    pages_info = []
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        while page <= max_pages:
            response = await client.get(
                f"{LUCIDBOT_BASE_URL}/users/find_by_custom_field",
                headers={"X-ACCESS-TOKEN": api_token, "Accept": "application/json"},
                params={"field_id": "728462", "value": ad_id, "page": page}
            )
            
            if response.status_code != 200:
                pages_info.append({"page": page, "error": response.status_code})
                break
            
            contacts = response.json().get("data", [])
            pages_info.append({"page": page, "contacts": len(contacts)})
            
            if not contacts:
                break
            
            all_contacts.extend(contacts)
            
            if len(contacts) < 100:
                break
            
            page += 1
    
    # Analizar contactos - detectando duplicados
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    # Detectar duplicados
    contact_ids_seen = set()
    duplicados = 0
    
    # Análisis de campos de venta
    campos_venta = {
        "con_total_pagar": 0,
        "con_calificacion": {},
        "con_estado": {},
        "total_pagar_fuera_rango": 0
    }
    
    analysis = {
        "total_contactos": len(all_contacts),
        "en_rango": 0,
        "fuera_rango": 0,
        "sin_fecha": 0,
        "ventas_en_rango": 0,
        "leads_en_rango": 0,
        "ventas_por_dia": {},
        "contactos_por_dia": {},
        "ejemplos_ventas": [],
        "ejemplos_filtrados": []
    }
    
    for contact in all_contacts:
        # Detectar duplicado
        contact_id = contact.get("id")
        if contact_id in contact_ids_seen:
            duplicados += 1
            continue
        contact_ids_seen.add(contact_id)
        
        custom_fields = contact.get("custom_fields", {})
        
        # Analizar campos de venta (sin importar fecha)
        total_pagar = custom_fields.get("Total a pagar")
        calificacion = custom_fields.get("Calificacion_LucidSales", "")
        estado = custom_fields.get("Estado_LucidSales", "")
        
        if total_pagar:
            campos_venta["con_total_pagar"] += 1
        
        if calificacion:
            if calificacion not in campos_venta["con_calificacion"]:
                campos_venta["con_calificacion"][calificacion] = 0
            campos_venta["con_calificacion"][calificacion] += 1
        
        if estado:
            if estado not in campos_venta["con_estado"]:
                campos_venta["con_estado"][estado] = 0
            campos_venta["con_estado"][estado] += 1
        created_at = contact.get("created_at", "")
        total_pagar = custom_fields.get("Total a pagar")
        
        if not created_at:
            analysis["sin_fecha"] += 1
            continue
        
        try:
            # Parsear fecha
            if " " in created_at:
                date_part = created_at.split(" ")[0]
            elif "T" in created_at:
                date_part = created_at.split("T")[0]
            else:
                date_part = created_at[:10]
            
            contact_date = datetime.strptime(date_part, "%Y-%m-%d").date()
            fecha_str = contact_date.strftime("%Y-%m-%d")
            
            # Contar por día
            if fecha_str not in analysis["contactos_por_dia"]:
                analysis["contactos_por_dia"][fecha_str] = 0
                analysis["ventas_por_dia"][fecha_str] = 0
            analysis["contactos_por_dia"][fecha_str] += 1
            
            # Verificar rango
            if start <= contact_date <= end:
                analysis["en_rango"] += 1
                
                if total_pagar:
                    try:
                        float(total_pagar)
                        analysis["ventas_en_rango"] += 1
                        analysis["ventas_por_dia"][fecha_str] += 1
                        
                        if len(analysis["ejemplos_ventas"]) < 5:
                            analysis["ejemplos_ventas"].append({
                                "nombre": contact.get("full_name", "")[:25],
                                "fecha": created_at,
                                "total_pagar": total_pagar,
                                "calificacion": custom_fields.get("Calificacion_LucidSales", "")
                            })
                    except:
                        analysis["leads_en_rango"] += 1
                else:
                    analysis["leads_en_rango"] += 1
            else:
                analysis["fuera_rango"] += 1
                if len(analysis["ejemplos_filtrados"]) < 3:
                    analysis["ejemplos_filtrados"].append({
                        "fecha": created_at,
                        "fecha_parseada": fecha_str,
                        "tiene_total_pagar": bool(total_pagar)
                    })
                    
        except Exception as e:
            analysis["sin_fecha"] += 1
    
    return {
        "ad_id": ad_id,
        "user_id": user_id,
        "rango_consultado": f"{start_date} a {end_date}",
        "paginacion": {
            "paginas": len(pages_info),
            "total_contactos": len(all_contacts),
            "contactos_unicos": len(contact_ids_seen),
            "duplicados": duplicados,
            "detalle": pages_info[:5]  # Solo primeras 5 páginas para no saturar
        },
        "analisis_campos_venta": campos_venta,
        "resumen": {
            "ventas_en_rango": analysis["ventas_en_rango"],
            "leads_en_rango": analysis["leads_en_rango"],
            "total_en_rango": analysis["en_rango"],
            "fuera_de_rango": analysis["fuera_rango"],
            "sin_fecha": analysis["sin_fecha"]
        },
        "ventas_por_dia": analysis["ventas_por_dia"],
        "contactos_por_dia": analysis["contactos_por_dia"],
        "ejemplos_ventas": analysis["ejemplos_ventas"],
        "ejemplos_filtrados": analysis["ejemplos_filtrados"]
    }
    
    if not lucidbot_conn:
        return {
            "error": "No hay conexión activa",
            "todas_conexiones": conexiones_info
        }
    
    api_token = decrypt_token(lucidbot_conn.api_token_encrypted)
    
    resultados = {
        "conexion_activa": {
            "id": lucidbot_conn.id,
            "user_id": lucidbot_conn.user_id,
            "account_id": lucidbot_conn.account_id
        },
        "todas_conexiones": conexiones_info,
        "pruebas_api": {}
    }
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        # Prueba 1: /account (info de cuenta)
        try:
            resp = await client.get(
                f"{LUCIDBOT_BASE_URL}/account",
                headers={"X-ACCESS-TOKEN": api_token, "Accept": "application/json"}
            )
            resultados["pruebas_api"]["account"] = {
                "status": resp.status_code,
                "data": resp.json() if resp.status_code == 200 else resp.text[:200]
            }
        except Exception as e:
            resultados["pruebas_api"]["account"] = {"error": str(e)}
        
        # Prueba 2: /users (contactos)
        try:
            resp = await client.get(
                f"{LUCIDBOT_BASE_URL}/users",
                headers={"X-ACCESS-TOKEN": api_token, "Accept": "application/json"},
                params={"limit": 5}
            )
            data = resp.json() if resp.status_code == 200 else resp.text[:200]
            resultados["pruebas_api"]["users"] = {
                "status": resp.status_code,
                "total_contactos": len(data.get("data", [])) if isinstance(data, dict) else 0,
                "response_keys": list(data.keys()) if isinstance(data, dict) else None
            }
        except Exception as e:
            resultados["pruebas_api"]["users"] = {"error": str(e)}
        
        # Prueba 3: /users/find_by_custom_field con un Ad ID conocido
        try:
            resp = await client.get(
                f"{LUCIDBOT_BASE_URL}/users/find_by_custom_field",
                headers={"X-ACCESS-TOKEN": api_token, "Accept": "application/json"},
                params={"field_id": "728462", "value": "120236155688730647"}
            )
            data = resp.json() if resp.status_code == 200 else resp.text[:200]
            resultados["pruebas_api"]["find_by_ad_id"] = {
                "status": resp.status_code,
                "total_contactos": len(data.get("data", [])) if isinstance(data, dict) else 0,
                "response_keys": list(data.keys()) if isinstance(data, dict) else None,
                "raw_response": data if resp.status_code != 200 else None
            }
        except Exception as e:
            resultados["pruebas_api"]["find_by_ad_id"] = {"error": str(e)}
        
        # Prueba 4: Buscar con el otro campo de ad_id (892889 = ad_id_lucidsales)
        try:
            resp = await client.get(
                f"{LUCIDBOT_BASE_URL}/users/find_by_custom_field",
                headers={"X-ACCESS-TOKEN": api_token, "Accept": "application/json"},
                params={"field_id": "892889", "value": "120236155688730647"}
            )
            data = resp.json() if resp.status_code == 200 else resp.text[:200]
            resultados["pruebas_api"]["find_by_ad_id_lucidsales"] = {
                "status": resp.status_code,
                "total_contactos": len(data.get("data", [])) if isinstance(data, dict) else 0
            }
        except Exception as e:
            resultados["pruebas_api"]["find_by_ad_id_lucidsales"] = {"error": str(e)}
    
    return resultados
    
    if not lucidbot_conn:
        return {"error": "LucidBot no conectado"}
    
    api_token = decrypt_token(lucidbot_conn.api_token_encrypted)
    
    # Obtener TODOS los contactos con paginación
    all_contacts = []
    page = 1
    max_pages = 20
    pages_info = []
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        while page <= max_pages:
            response = await client.get(
                f"{LUCIDBOT_BASE_URL}/users/find_by_custom_field",
                headers={"X-ACCESS-TOKEN": api_token, "Accept": "application/json"},
                params={"field_id": "728462", "value": ad_id, "page": page}
            )
            
            if response.status_code != 200:
                pages_info.append({"page": page, "error": response.status_code})
                break
            
            contacts = response.json().get("data", [])
            pages_info.append({"page": page, "contacts": len(contacts)})
            
            if not contacts:
                break
            
            all_contacts.extend(contacts)
            
            if len(contacts) < 100:
                break
            
            page += 1
    
    # Analizar contactos
    def analyze_for_range(contacts, start_date, end_date):
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        result = {
            "rango": f"{start_date} a {end_date}",
            "total_contactos": len(contacts),
            "en_rango": 0,
            "fuera_rango": 0,
            "sin_fecha": 0,
            "ventas_en_rango": 0,
            "leads_en_rango": 0,
            "fechas_contactos": {},
            "ejemplos_filtrados": [],
            "ejemplos_ventas": []
        }
        
        for contact in contacts:
            custom_fields = contact.get("custom_fields", {})
            created_at = contact.get("created_at", "")
            total_pagar = custom_fields.get("Total a pagar")
            
            if not created_at:
                result["sin_fecha"] += 1
                continue
            
            try:
                # Parsear fecha - solo la parte de fecha
                if " " in created_at:
                    date_part = created_at.split(" ")[0]
                elif "T" in created_at:
                    date_part = created_at.split("T")[0]
                else:
                    date_part = created_at[:10]
                
                contact_date = datetime.strptime(date_part, "%Y-%m-%d").date()
                
                # Contar por fecha
                fecha_str = contact_date.strftime("%Y-%m-%d")
                if fecha_str not in result["fechas_contactos"]:
                    result["fechas_contactos"][fecha_str] = {"total": 0, "ventas": 0}
                result["fechas_contactos"][fecha_str]["total"] += 1
                
                # Verificar rango
                if start <= contact_date <= end:
                    result["en_rango"] += 1
                    if total_pagar:
                        try:
                            float(total_pagar)
                            result["ventas_en_rango"] += 1
                            result["fechas_contactos"][fecha_str]["ventas"] += 1
                            if len(result["ejemplos_ventas"]) < 3:
                                result["ejemplos_ventas"].append({
                                    "nombre": contact.get("full_name", "")[:25],
                                    "fecha": created_at,
                                    "total_pagar": total_pagar
                                })
                        except:
                            result["leads_en_rango"] += 1
                    else:
                        result["leads_en_rango"] += 1
                else:
                    result["fuera_rango"] += 1
                    if len(result["ejemplos_filtrados"]) < 3:
                        result["ejemplos_filtrados"].append({
                            "fecha": created_at,
                            "fecha_parseada": fecha_str,
                            "tiene_venta": bool(total_pagar)
                        })
                        
            except Exception as e:
                result["sin_fecha"] += 1
        
        return result
    
    # Comparar diferentes rangos
    analisis_dia = analyze_for_range(all_contacts, "2025-12-13", "2025-12-13")
    analisis_semana = analyze_for_range(all_contacts, "2025-12-01", "2025-12-07")
    analisis_mes = analyze_for_range(all_contacts, "2025-12-01", "2025-12-15")
    
    return {
        "ad_id": ad_id,
        "paginacion": {
            "paginas_consultadas": len(pages_info),
            "total_contactos_obtenidos": len(all_contacts),
            "detalle_paginas": pages_info
        },
        "comparacion": {
            "un_dia_13dic": {
                "ventas": analisis_dia["ventas_en_rango"],
                "leads": analisis_dia["leads_en_rango"],
                "en_rango": analisis_dia["en_rango"]
            },
            "semana_1_7dic": {
                "ventas": analisis_semana["ventas_en_rango"],
                "leads": analisis_semana["leads_en_rango"],
                "en_rango": analisis_semana["en_rango"]
            },
            "mes_1_15dic": {
                "ventas": analisis_mes["ventas_en_rango"],
                "leads": analisis_mes["leads_en_rango"],
                "en_rango": analisis_mes["en_rango"]
            }
        },
        "distribucion_fechas": analisis_mes["fechas_contactos"],
        "detalle_semana": analisis_semana,
        "ejemplos_filtrados_semana": analisis_semana["ejemplos_filtrados"],
        "ejemplos_ventas_semana": analisis_semana["ejemplos_ventas"]
    }
