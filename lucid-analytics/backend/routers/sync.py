"""
Router de Sincronización
Sincroniza contactos de LucidBot a la base de datos local
para superar el límite de 100 contactos de la API.
"""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List, Optional
from datetime import datetime, timedelta
import httpx

from database import get_db, User, MetaAccount, LucidbotConnection, LucidbotContact
from routers.auth import get_current_user
from utils import decrypt_token

router = APIRouter()

LUCIDBOT_BASE_URL = "https://panel.lucidbot.co/api"
AD_FIELD_ID = "728462"  # ID del campo "Anuncio Facebook" en LucidBot


async def fetch_lucidbot_contacts(api_token: str, ad_id: str) -> List[dict]:
    """
    Obtener contactos de LucidBot para un ad_id específico.
    Nota: La API tiene límite de 100, pero capturamos lo que podamos.
    """
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{LUCIDBOT_BASE_URL}/users/find_by_custom_field",
            headers={
                "X-ACCESS-TOKEN": api_token,
                "Accept": "application/json"
            },
            params={
                "field_id": AD_FIELD_ID,
                "value": ad_id
            },
            timeout=30
        )
        
        if response.status_code != 200:
            return []
        
        return response.json().get("data", [])


def parse_lucidbot_datetime(date_str: str) -> Optional[datetime]:
    """Parsear fecha de LucidBot a datetime"""
    if not date_str:
        return None
    
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%SZ",
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str).strip(), fmt)
        except ValueError:
            continue
    
    return None


def sync_contacts_to_db(
    db: Session, 
    user_id: int, 
    contacts: List[dict], 
    ad_id: str
) -> dict:
    """
    Sincronizar contactos a la base de datos.
    Retorna estadísticas de la sincronización.
    """
    new_count = 0
    updated_count = 0
    skipped_count = 0
    
    try:
        for contact in contacts:
            lucidbot_id = contact.get("id")
            if not lucidbot_id:
                skipped_count += 1
                continue
            
            # Buscar si ya existe
            existing = db.query(LucidbotContact).filter(
                LucidbotContact.lucidbot_id == lucidbot_id
            ).first()
            
            # Obtener custom_fields
            custom_fields = contact.get("custom_fields", {})
            
            # Parsear total_a_pagar
            total_a_pagar = None
            total_str = custom_fields.get("Total a pagar")
            if total_str:
                try:
                    total_a_pagar = float(total_str)
                except (ValueError, TypeError):
                    pass
            
            # Parsear fecha de creación
            contact_created_at = parse_lucidbot_datetime(contact.get("created_at"))
            if not contact_created_at:
                skipped_count += 1
                continue
            
            if existing:
                # Actualizar si cambió algo importante
                if existing.total_a_pagar != total_a_pagar:
                    existing.total_a_pagar = total_a_pagar
                    existing.producto = custom_fields.get("Producto_Ordenados", "")
                    existing.calificacion = custom_fields.get("Calificacion_LucidSales", "")
                    existing.updated_at = datetime.utcnow()
                    updated_count += 1
                else:
                    skipped_count += 1
            else:
                # Crear nuevo
                new_contact = LucidbotContact(
                    user_id=user_id,
                    lucidbot_id=lucidbot_id,
                    full_name=contact.get("full_name", ""),
                    phone=contact.get("phone", ""),
                    ad_id=ad_id,
                    total_a_pagar=total_a_pagar,
                    producto=custom_fields.get("Producto_Ordenados", ""),
                    calificacion=custom_fields.get("Calificacion_LucidSales", ""),
                    contact_created_at=contact_created_at,
                    synced_at=datetime.utcnow()
                )
                db.add(new_contact)
                new_count += 1
        
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"[SYNC] Error in sync_contacts_to_db: {e}")
        raise e
    
    return {
        "new": new_count,
        "updated": updated_count,
        "skipped": skipped_count
    }


async def get_meta_ad_ids(access_token: str, account_id: str) -> List[str]:
    """Obtener lista de ad_ids de una cuenta de Meta"""
    ad_ids = []
    async with httpx.AsyncClient(timeout=60) as client:
        response = await client.get(
            f"https://graph.facebook.com/v21.0/act_{account_id}/ads",
            params={
                "access_token": access_token,
                "fields": "id",
                "limit": 500
            }
        )
        if response.status_code == 200:
            ads = response.json().get("data", [])
            ad_ids = [ad.get("id") for ad in ads if ad.get("id")]
    return ad_ids


@router.post("/lucidbot")
async def sync_lucidbot_contacts(
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Sincronizar contactos de LucidBot a la base de datos local.
    Esto obtiene los últimos 100 contactos de cada anuncio activo.
    Obtiene los ad_ids automáticamente desde Meta Ads.
    """
    try:
        # Limpiar cualquier transacción fallida previa
        db.rollback()
        
        # Verificar conexión LucidBot
        lucidbot_conn = db.query(LucidbotConnection).filter(
            LucidbotConnection.user_id == current_user.id,
            LucidbotConnection.is_active == True
        ).first()
        
        if not lucidbot_conn:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No hay conexión activa con LucidBot"
            )
        
        lucidbot_token = decrypt_token(lucidbot_conn.api_token_encrypted)
        
        # Obtener todos los ad_ids únicos que ya tenemos en la DB
        existing_ad_ids = db.query(LucidbotContact.ad_id).filter(
            LucidbotContact.user_id == current_user.id,
            LucidbotContact.ad_id != None
        ).distinct().all()
        
        ad_ids_to_sync = set([a[0] for a in existing_ad_ids if a[0]])
        
        # IMPORTANTE: También obtener ad_ids de Meta Accounts activos
        meta_accounts = db.query(MetaAccount).filter(
            MetaAccount.user_id == current_user.id,
            MetaAccount.is_active == True
        ).all()
        
        for meta_account in meta_accounts:
            try:
                meta_token = decrypt_token(meta_account.access_token_encrypted)
                meta_ad_ids = await get_meta_ad_ids(meta_token, meta_account.account_id)
                ad_ids_to_sync.update(meta_ad_ids)
                print(f"[SYNC] Found {len(meta_ad_ids)} ads from Meta account {meta_account.account_id}")
            except Exception as e:
                print(f"[SYNC] Error getting ads from Meta account {meta_account.account_id}: {e}")
                continue
        
        print(f"[SYNC] Total ad_ids to sync: {len(ad_ids_to_sync)}")
        
        # Estadísticas totales
        total_stats = {
            "ads_synced": 0,
            "new_contacts": 0,
            "updated_contacts": 0,
            "skipped_contacts": 0,
            "ads_with_contacts": 0,
            "errors": 0
        }
        
        # Sincronizar cada ad_id conocido
        for ad_id in ad_ids_to_sync:
            try:
                contacts = await fetch_lucidbot_contacts(lucidbot_token, ad_id)
                if contacts:
                    stats = sync_contacts_to_db(db, current_user.id, contacts, ad_id)
                    total_stats["ads_synced"] += 1
                    total_stats["new_contacts"] += stats["new"]
                    total_stats["updated_contacts"] += stats["updated"]
                    total_stats["skipped_contacts"] += stats["skipped"]
                    if stats["new"] > 0 or stats["updated"] > 0:
                        total_stats["ads_with_contacts"] += 1
                    print(f"[SYNC] Ad {ad_id}: {len(contacts)} contacts, {stats['new']} new, {stats['updated']} updated")
            except Exception as e:
                print(f"[SYNC] Error syncing ad_id {ad_id}: {e}")
                db.rollback()  # Rollback para continuar con el siguiente
                total_stats["errors"] += 1
                continue
        
        # Contar total de contactos en DB
        total_in_db = db.query(func.count(LucidbotContact.id)).filter(
            LucidbotContact.user_id == current_user.id
        ).scalar()
        
        return {
            "success": True,
            "message": "Sincronización completada",
            "stats": total_stats,
            "total_contacts_in_db": total_in_db
        }
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        print(f"[SYNC] Critical error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error en sincronización: {str(e)}"
        )


@router.post("/lucidbot/ad/{ad_id}")
async def sync_single_ad(
    ad_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Sincronizar contactos de un anuncio específico.
    Útil para agregar nuevos anuncios al sistema.
    """
    try:
        # Limpiar cualquier transacción fallida previa
        db.rollback()
        
        # Verificar conexión LucidBot
        lucidbot_conn = db.query(LucidbotConnection).filter(
            LucidbotConnection.user_id == current_user.id,
            LucidbotConnection.is_active == True
        ).first()
        
        if not lucidbot_conn:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No hay conexión activa con LucidBot"
            )
        
        lucidbot_token = decrypt_token(lucidbot_conn.api_token_encrypted)
        
        print(f"[SYNC] Syncing single ad: {ad_id}")
        print(f"[SYNC] Using token: {lucidbot_token[:20]}...")
        
        # Obtener contactos de este ad_id
        contacts = await fetch_lucidbot_contacts(lucidbot_token, ad_id)
        
        print(f"[SYNC] Contacts found: {len(contacts) if contacts else 0}")
        
        if not contacts:
            return {
                "success": True,
                "message": "No se encontraron contactos para este anuncio",
                "ad_id": ad_id,
                "contacts_found": 0
            }
        
        # Sincronizar
        stats = sync_contacts_to_db(db, current_user.id, contacts, ad_id)
        
        return {
            "success": True,
            "message": f"Sincronización completada para anuncio {ad_id}",
            "ad_id": ad_id,
            "contacts_found": len(contacts),
            "stats": stats
        }
    
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        print(f"[SYNC] Error in sync_single_ad: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error sincronizando anuncio: {str(e)}"
        )


@router.get("/lucidbot/stats")
async def get_sync_stats(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener estadísticas de contactos sincronizados"""
    
    total_contacts = db.query(LucidbotContact).filter(
        LucidbotContact.user_id == current_user.id
    ).count()
    
    total_sales = db.query(LucidbotContact).filter(
        LucidbotContact.user_id == current_user.id,
        LucidbotContact.total_a_pagar != None,
        LucidbotContact.total_a_pagar > 0
    ).count()
    
    unique_ads = db.query(func.count(func.distinct(LucidbotContact.ad_id))).filter(
        LucidbotContact.user_id == current_user.id
    ).scalar()
    
    # Última sincronización
    last_sync = db.query(func.max(LucidbotContact.synced_at)).filter(
        LucidbotContact.user_id == current_user.id
    ).scalar()
    
    # Contactos por fecha
    oldest = db.query(func.min(LucidbotContact.contact_created_at)).filter(
        LucidbotContact.user_id == current_user.id
    ).scalar()
    
    newest = db.query(func.max(LucidbotContact.contact_created_at)).filter(
        LucidbotContact.user_id == current_user.id
    ).scalar()
    
    return {
        "total_contacts": total_contacts,
        "total_sales": total_sales,
        "unique_ads": unique_ads,
        "last_sync": last_sync.isoformat() if last_sync else None,
        "date_range": {
            "oldest": oldest.isoformat() if oldest else None,
            "newest": newest.isoformat() if newest else None
        }
    }


@router.delete("/lucidbot/clear")
async def clear_sync_data(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Limpiar todos los datos sincronizados (usar con cuidado).
    Útil para re-sincronizar desde cero.
    """
    deleted = db.query(LucidbotContact).filter(
        LucidbotContact.user_id == current_user.id
    ).delete()
    
    db.commit()
    
    return {
        "success": True,
        "message": f"Se eliminaron {deleted} contactos sincronizados"
    }
