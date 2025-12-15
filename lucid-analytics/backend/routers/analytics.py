"""
Router de Analytics v3
Combina datos de Meta + LucidBot para calcular CPA, ROAS, etc.

IMPORTANTE: Este módulo sincroniza automáticamente los contactos de LucidBot
antes de consultar, eliminando la necesidad de sincronización manual.
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from typing import List, Optional
from datetime import datetime, timedelta
import httpx
import re

from database import get_db, User, MetaAccount, LucidbotConnection, LucidbotContact
from routers.auth import get_current_user
from utils import decrypt_token

router = APIRouter()

META_API_VERSION = "v21.0"
META_BASE_URL = f"https://graph.facebook.com/{META_API_VERSION}"
LUCIDBOT_BASE_URL = "https://panel.lucidbot.co/api"

# Offset UTC a Colombia
UTC_OFFSET_HOURS = 5


# ========== HELPERS ==========

def get_utc_date_range(start_date: str, end_date: str) -> tuple:
    """
    Convertir rango de fechas Colombia a UTC.
    """
    start_utc = datetime.strptime(start_date, "%Y-%m-%d") + timedelta(hours=UTC_OFFSET_HOURS)
    end_utc = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59) + timedelta(hours=UTC_OFFSET_HOURS)
    return start_utc, end_utc


async def fetch_lucidbot_contacts(api_token: str, ad_id: str, ad_field_id: str = "728462") -> list:
    """
    Obtener TODOS los contactos de LucidBot para un anuncio específico.
    Usa paginación para superar el límite de 100 por página.
    """
    all_contacts = []
    page = 1
    max_pages = 50  # Límite de seguridad para evitar loops infinitos
    
    async with httpx.AsyncClient(timeout=60) as client:
        while page <= max_pages:
            try:
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
                    }
                )
                
                if response.status_code == 200:
                    data = response.json()
                    contacts = data.get("data", [])
                    
                    if not contacts:
                        # No más contactos, salir del loop
                        break
                    
                    all_contacts.extend(contacts)
                    print(f"[LUCIDBOT] ad_id={ad_id} page={page} contacts={len(contacts)} total={len(all_contacts)}")
                    
                    if len(contacts) < 100:
                        # Última página (menos de 100 resultados)
                        break
                    
                    page += 1
                else:
                    print(f"[LUCIDBOT] ad_id={ad_id} page={page} status={response.status_code} error={response.text[:100]}")
                    break
                    
            except Exception as e:
                print(f"[LUCIDBOT] Error fetching ad {ad_id} page {page}: {e}")
                break
    
    print(f"[LUCIDBOT] ad_id={ad_id} TOTAL={len(all_contacts)} contacts from {page} pages")
    return all_contacts


def sync_contacts_to_db(db: Session, user_id: int, contacts: list, ad_id: str) -> dict:
    """Sincronizar contactos a la base de datos local."""
    stats = {"new": 0, "updated": 0, "skipped": 0}
    
    for contact in contacts:
        try:
            lucidbot_id = contact.get("id")
            if not lucidbot_id:
                stats["skipped"] += 1
                continue
            
            # Buscar si ya existe
            existing = db.query(LucidbotContact).filter(
                LucidbotContact.lucidbot_id == int(lucidbot_id)
            ).first()
            
            # Parsear fecha - usar created_at del contacto
            created_at_str = contact.get("created_at", "")
            try:
                if created_at_str:
                    if "T" in str(created_at_str):
                        contact_created_at = datetime.fromisoformat(str(created_at_str).replace("Z", "+00:00").replace("+00:00", ""))
                    else:
                        contact_created_at = datetime.strptime(str(created_at_str), "%Y-%m-%d %H:%M:%S")
                else:
                    contact_created_at = datetime.utcnow()
            except:
                contact_created_at = datetime.utcnow()
            
            # Obtener custom_fields (formato real de LucidBot)
            custom_fields = contact.get("custom_fields", {})
            
            # Extraer total_a_pagar desde custom_fields
            total_a_pagar = None
            total_str = custom_fields.get("Total a pagar") or custom_fields.get("total_a_pagar")
            if total_str:
                try:
                    # Puede ser string con formato "123456" o float
                    cleaned = str(total_str).replace("$", "").replace(",", "").replace(".", "").strip()
                    if cleaned and cleaned.isdigit():
                        total_a_pagar = float(cleaned)
                    else:
                        total_a_pagar = float(total_str)
                except:
                    pass
            
            # Producto y calificación desde custom_fields
            producto = custom_fields.get("Producto_Ordenados") or custom_fields.get("producto") or ""
            calificacion = custom_fields.get("Calificacion_LucidSales") or custom_fields.get("calificacion") or ""
            
            if existing:
                # Actualizar si hay cambios importantes
                if total_a_pagar and not existing.total_a_pagar:
                    existing.total_a_pagar = total_a_pagar
                    existing.producto = producto
                    existing.calificacion = calificacion
                    existing.updated_at = datetime.utcnow()
                    stats["updated"] += 1
                else:
                    stats["skipped"] += 1
            else:
                # Crear nuevo
                new_contact = LucidbotContact(
                    user_id=user_id,
                    lucidbot_id=int(lucidbot_id),
                    full_name=contact.get("full_name") or contact.get("name") or "",
                    phone=contact.get("phone") or "",
                    ad_id=ad_id,
                    total_a_pagar=total_a_pagar,
                    producto=producto,
                    calificacion=calificacion,
                    contact_created_at=contact_created_at,
                    synced_at=datetime.utcnow()
                )
                db.add(new_contact)
                stats["new"] += 1
        except Exception as e:
            print(f"[SYNC] Error processing contact: {e}")
            stats["skipped"] += 1
            continue
    
    try:
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"[SYNC] Error committing: {e}")
        raise e
    
    return stats


async def auto_sync_for_ads(db: Session, user_id: int, ad_ids: list, lucidbot_token: str) -> dict:
    """
    Sincronización automática para una lista de anuncios.
    Se ejecuta antes de consultar el dashboard.
    """
    total_stats = {"synced": 0, "new": 0, "updated": 0, "errors": 0}
    
    for ad_id in ad_ids:
        try:
            contacts = await fetch_lucidbot_contacts(lucidbot_token, ad_id)
            if contacts:
                stats = sync_contacts_to_db(db, user_id, contacts, ad_id)
                total_stats["new"] += stats["new"]
                total_stats["updated"] += stats["updated"]
                total_stats["synced"] += 1
        except Exception as e:
            print(f"[AUTO-SYNC] Error syncing ad {ad_id}: {e}")
            db.rollback()
            total_stats["errors"] += 1
            continue
    
    return total_stats


def get_lucidbot_data_from_db(
    db: Session,
    user_id: int,
    ad_id: str,
    start_date: str,
    end_date: str
) -> dict:
    """
    Obtener datos de LucidBot desde la base de datos local.
    """
    start_utc, end_utc = get_utc_date_range(start_date, end_date)
    
    contacts = db.query(LucidbotContact).filter(
        and_(
            LucidbotContact.user_id == user_id,
            LucidbotContact.ad_id == ad_id,
            LucidbotContact.contact_created_at >= start_utc,
            LucidbotContact.contact_created_at <= end_utc
        )
    ).all()
    
    leads = 0
    sales = 0
    revenue = 0.0
    contact_details = []
    
    for contact in contacts:
        contact_info = {
            "name": contact.full_name,
            "phone": contact.phone,
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
        "leads": leads,
        "sales": sales,
        "revenue": revenue,
        "contacts": contact_details,
        "_debug": {
            "source": "local_db",
            "total_contacts": len(contacts),
            "date_range_utc": {
                "start": start_utc.isoformat(),
                "end": end_utc.isoformat()
            }
        }
    }


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


# ========== ENDPOINTS ==========

@router.get("/dashboard")
async def get_dashboard(
    account_id: str,
    start_date: str,
    end_date: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Dashboard principal: métricas combinadas Meta + LucidBot
    
    SINCRONIZACIÓN AUTOMÁTICA: Los datos de LucidBot se sincronizan
    automáticamente antes de calcular las métricas.
    """
    
    # Limpiar cualquier transacción fallida
    db.rollback()
    
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
    
    # ========== SINCRONIZACIÓN AUTOMÁTICA ==========
    sync_stats = None
    lucidbot_connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if lucidbot_connection:
        try:
            lucidbot_token = decrypt_token(lucidbot_connection.api_token_encrypted)
            ad_ids = [ad.get("ad_id") for ad in meta_ads if ad.get("ad_id")]
            
            print(f"[AUTO-SYNC] User {current_user.id} - Token: {lucidbot_token[:20]}... - Ads: {len(ad_ids)}")
            print(f"[AUTO-SYNC] Ad IDs: {ad_ids[:5]}...")  # Mostrar primeros 5
            sync_stats = await auto_sync_for_ads(db, current_user.id, ad_ids, lucidbot_token)
            print(f"[AUTO-SYNC] Done: {sync_stats}")
        except Exception as e:
            print(f"[AUTO-SYNC] Error: {e}")
            db.rollback()
    # ========== FIN SINCRONIZACIÓN ==========
    
    ads_analytics = []
    total_spend = 0
    total_revenue = 0
    total_leads = 0
    total_sales = 0
    
    for ad in meta_ads:
        ad_id = ad.get("ad_id")
        spend = float(ad.get("spend", 0))
        
        # Obtener datos de LucidBot DESDE LA DB LOCAL
        lucid_data = get_lucidbot_data_from_db(
            db=db,
            user_id=current_user.id,
            ad_id=ad_id,
            start_date=start_date,
            end_date=end_date
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
    
    # Verificar si hay datos sincronizados
    total_synced = db.query(func.count(LucidbotContact.id)).filter(
        LucidbotContact.user_id == current_user.id
    ).scalar()
    
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
            "total_contacts_synced": total_synced,
            "data_source": "local_db",
            "auto_sync": sync_stats
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
