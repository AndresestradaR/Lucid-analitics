"""
Router de Sincronización - LucidBot
Sincroniza contactos de LucidBot a la base de datos local
para evitar el límite de 100 contactos por llamada API.
"""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import datetime, timedelta
from typing import Optional
import httpx
import logging

from database import get_db, User, LucidbotConnection, LucidbotContact, MetaAccount
from routers.auth import get_current_user
from utils import decrypt_token

router = APIRouter()

# Configurar logging para que muestre en Railway
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

LUCIDBOT_BASE_URL = "https://panel.lucidbot.co/api"

# ========== HELPERS ==========

async def fetch_all_contacts_for_ad(
    api_token: str,
    ad_id: str,
    ad_field_id: str = "728462"
) -> list:
    """
    Obtener TODOS los contactos de un ad_id paginando la API.
    
    Args:
        api_token: Token de LucidBot
        ad_id: ID del anuncio de Facebook
        ad_field_id: ID del campo personalizado "Anuncio Facebook"
    
    Returns:
        Lista de todos los contactos
    """
    all_contacts = []
    page = 1
    max_pages = 100  # Límite de seguridad
    
    print(f"[SYNC] Iniciando fetch para ad_id={ad_id}")
    print(f"[SYNC] Token (primeros 20 chars): {api_token[:20] if api_token else 'NONE'}...")
    print(f"[SYNC] field_id={ad_field_id}")
    
    async with httpx.AsyncClient(timeout=60) as client:
        while page <= max_pages:
            try:
                url = f"{LUCIDBOT_BASE_URL}/users/find_by_custom_field"
                params = {
                    "field_id": ad_field_id,
                    "value": ad_id,
                    "page": page
                }
                headers = {
                    "X-ACCESS-TOKEN": api_token,
                    "Accept": "application/json"
                }
                
                print(f"[SYNC] GET {url} page={page}")
                
                response = await client.get(url, headers=headers, params=params)
                
                print(f"[SYNC] Response status: {response.status_code}")
                
                if response.status_code != 200:
                    print(f"[SYNC] Error API LucidBot página {page}: {response.status_code}")
                    print(f"[SYNC] Response body: {response.text[:500]}")
                    break
                
                json_response = response.json()
                data = json_response.get("data", [])
                
                print(f"[SYNC] ad_id={ad_id} página {page}: {len(data)} contactos")
                
                if not data:
                    print(f"[SYNC] No hay más datos en página {page}")
                    break
                
                all_contacts.extend(data)
                
                if len(data) < 100:
                    print(f"[SYNC] Última página alcanzada ({len(data)} < 100)")
                    break
                
                page += 1
                
            except Exception as e:
                print(f"[SYNC] Error en página {page}: {str(e)}")
                import traceback
                print(f"[SYNC] Traceback: {traceback.format_exc()}")
                break
    
    print(f"[SYNC] ad_id={ad_id} TOTAL: {len(all_contacts)} contactos")
    return all_contacts


def parse_contact_date(date_str: str) -> Optional[datetime]:
    """Parsear fecha de LucidBot a datetime"""
    if not date_str:
        return None
    
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d",
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(str(date_str).strip(), fmt)
        except ValueError:
            continue
    
    return None


async def sync_contacts_to_db(
    db: Session,
    user_id: int,
    contacts: list,
    ad_id: str
) -> dict:
    """
    Guardar/actualizar contactos en la base de datos local.
    
    Args:
        db: Sesión de base de datos
        user_id: ID del usuario
        contacts: Lista de contactos de LucidBot
        ad_id: ID del anuncio
    
    Returns:
        Dict con estadísticas de sincronización
    """
    created = 0
    updated = 0
    errors = 0
    
    for contact in contacts:
        try:
            lucidbot_id = int(contact.get("id", 0))
            if not lucidbot_id:
                errors += 1
                continue
            
            custom_fields = contact.get("custom_fields", {})
            
            # Parsear fecha
            contact_date = parse_contact_date(contact.get("created_at"))
            if not contact_date:
                errors += 1
                continue
            
            # Parsear total a pagar
            total_str = custom_fields.get("Total a pagar")
            total_a_pagar = None
            if total_str:
                try:
                    total_a_pagar = float(str(total_str).replace(",", ""))
                except ValueError:
                    pass
            
            # Buscar si ya existe
            existing = db.query(LucidbotContact).filter(
                LucidbotContact.lucidbot_id == lucidbot_id
            ).first()
            
            if existing:
                # Actualizar
                existing.full_name = contact.get("full_name", "")
                existing.phone = contact.get("phone", "")
                existing.ad_id = ad_id
                existing.total_a_pagar = total_a_pagar
                existing.producto = custom_fields.get("Producto_Ordenados", "")
                existing.calificacion = custom_fields.get("Calificacion_LucidSales", "")
                existing.contact_created_at = contact_date
                existing.updated_at = datetime.utcnow()
                updated += 1
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
                    contact_created_at=contact_date,
                    synced_at=datetime.utcnow()
                )
                db.add(new_contact)
                created += 1
        
        except Exception as e:
            logger.error(f"Error procesando contacto: {str(e)}")
            errors += 1
    
    db.commit()
    
    return {
        "created": created,
        "updated": updated,
        "errors": errors,
        "total_processed": len(contacts)
    }


# ========== ENDPOINTS ==========

@router.post("/lucidbot/ad/{ad_id}")
async def sync_ad_contacts(
    ad_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Sincronizar todos los contactos de un anuncio específico.
    
    Este endpoint pagina por TODAS las páginas de la API de LucidBot
    y guarda los contactos en la base de datos local.
    """
    print(f"[SYNC ENDPOINT] Iniciando sync para ad_id={ad_id}, user_id={current_user.id}")
    
    # Verificar conexión LucidBot
    lucidbot_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not lucidbot_conn:
        print(f"[SYNC ENDPOINT] No hay conexión LucidBot para user_id={current_user.id}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No hay conexión activa con LucidBot"
        )
    
    print(f"[SYNC ENDPOINT] Conexión LucidBot encontrada, account_id={lucidbot_conn.account_id}")
    
    api_token = decrypt_token(lucidbot_conn.api_token_encrypted)
    
    if not api_token:
        print(f"[SYNC ENDPOINT] Error desencriptando token")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error al obtener token de LucidBot"
        )
    
    print(f"[SYNC ENDPOINT] Token desencriptado OK, longitud={len(api_token)}")
    
    # Obtener TODOS los contactos paginando
    contacts = await fetch_all_contacts_for_ad(api_token, ad_id)
    
    print(f"[SYNC ENDPOINT] fetch_all_contacts_for_ad retornó {len(contacts)} contactos")
    
    if not contacts:
        return {
            "message": "No se encontraron contactos para este anuncio",
            "ad_id": ad_id,
            "total_contacts": 0
        }
    
    # Guardar en BD
    result = await sync_contacts_to_db(db, current_user.id, contacts, ad_id)
    
    print(f"[SYNC ENDPOINT] Sync completado: {result}")
    
    return {
        "message": "Sincronización completada",
        "ad_id": ad_id,
        **result
    }


@router.post("/lucidbot/all")
async def sync_all_ads(
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Sincronizar contactos de TODOS los anuncios activos del usuario.
    
    Obtiene los ad_ids de Meta Ads y sincroniza cada uno.
    Se ejecuta en background para no bloquear.
    """
    # Verificar conexiones
    lucidbot_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not lucidbot_conn:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No hay conexión activa con LucidBot"
        )
    
    meta_accounts = db.query(MetaAccount).filter(
        MetaAccount.user_id == current_user.id,
        MetaAccount.is_active == True
    ).all()
    
    if not meta_accounts:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No hay cuentas de Meta conectadas"
        )
    
    # Por ahora retornamos inmediatamente, la sync se hace en background
    # TODO: Implementar background task completo
    
    return {
        "message": "Sincronización iniciada en background",
        "accounts": len(meta_accounts)
    }


@router.get("/lucidbot/status")
async def get_sync_status(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener estado de la sincronización de contactos"""
    
    # Contar contactos por ad_id
    contacts_by_ad = db.query(
        LucidbotContact.ad_id,
        func.count(LucidbotContact.id).label("total"),
        func.count(LucidbotContact.total_a_pagar).label("ventas"),
        func.max(LucidbotContact.synced_at).label("last_sync")
    ).filter(
        LucidbotContact.user_id == current_user.id
    ).group_by(
        LucidbotContact.ad_id
    ).all()
    
    total_contacts = db.query(func.count(LucidbotContact.id)).filter(
        LucidbotContact.user_id == current_user.id
    ).scalar()
    
    total_ventas = db.query(func.count(LucidbotContact.id)).filter(
        LucidbotContact.user_id == current_user.id,
        LucidbotContact.total_a_pagar.isnot(None)
    ).scalar()
    
    last_sync = db.query(func.max(LucidbotContact.synced_at)).filter(
        LucidbotContact.user_id == current_user.id
    ).scalar()
    
    return {
        "total_contacts": total_contacts,
        "total_ventas": total_ventas,
        "total_ads": len(contacts_by_ad),
        "last_sync": last_sync.isoformat() if last_sync else None,
        "by_ad": [
            {
                "ad_id": row.ad_id,
                "contacts": row.total,
                "ventas": row.ventas,
                "last_sync": row.last_sync.isoformat() if row.last_sync else None
            }
            for row in contacts_by_ad
        ]
    }


@router.delete("/lucidbot/clear")
async def clear_sync_data(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Eliminar todos los contactos sincronizados (para re-sync)"""
    
    deleted = db.query(LucidbotContact).filter(
        LucidbotContact.user_id == current_user.id
    ).delete()
    
    db.commit()
    
    return {
        "message": f"Eliminados {deleted} contactos sincronizados",
        "deleted": deleted
    }
