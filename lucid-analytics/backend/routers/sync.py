"""
Sincronización de contactos de LucidBot
Traemos TODOS los contactos por página y los guardamos localmente.

IMPORTANTE - Extracción de ad_id:
- El ad_id viene de los custom_fields de LucidBot
- Campo 728462: "Anuncio Facebook" - contiene ad_id directo
- Campo 764700: JSON del pedido - contiene {"ad": "123456789"}
- Se necesita una llamada adicional por contacto para obtener estos campos
"""

import httpx
from datetime import datetime, timedelta
from typing import Optional, List, Dict
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import text
import json
import asyncio

from database import SessionLocal, LucidbotConnection, LucidbotContact, User
from utils import decrypt_token

LUCIDBOT_PHP_URL = "https://panel.lucidbot.co/php/user.php"

# IDs de campos personalizados en LucidBot
AD_ID_FIELD = "728462"      # Campo "Anuncio Facebook" - ad_id directo
ORDER_JSON_FIELD = "764700"  # Campo JSON del pedido - contiene {"ad": "..."}
ESTADO_FIELD = "926799"      # Campo de estado/calificación
TOTAL_FIELD = "117867"       # Campo total a pagar
PRODUCTO_FIELD = "116501"    # Campo producto


async def fetch_contact_custom_fields(
    jwt_token: str,
    page_id: str,
    contact_id: str
) -> Dict:
    """
    Obtener custom_fields detallados de UN contacto específico.
    
    Esta es la llamada que obtiene el ad_id que está en:
    - Campo 728462 (Anuncio Facebook) - directo
    - Campo 764700 (JSON del pedido) - {"ad": "123456789"}
    
    Returns:
        Dict con ad_id, total_a_pagar, producto, calificacion
    """
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"token={jwt_token}; last_page_id={page_id}"
    }
    
    payload = [{
        "op": "users",
        "op1": "get",
        "ms_id": contact_id,
        "expand": {"boards": True},
        "pageName": "inbox",
        "page_id": page_id
    }]
    
    result = {
        "ad_id": None,
        "total_a_pagar": None,
        "producto": None,
        "calificacion": None
    }
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                LUCIDBOT_PHP_URL,
                headers=headers,
                json=payload
            )
            
            if response.status_code != 200:
                return result
            
            data = response.json()
            
            if not data or len(data) == 0:
                return result
            
            user_data = data[0].get("data", {})
            custom_fields = user_data.get("custom_fields", [])
            
            # Procesar cada custom field
            for cf in custom_fields:
                field_id = str(cf.get("id", ""))
                value = cf.get("value", "")
                
                if not value:
                    continue
                
                # Campo 728462: ad_id directo
                if field_id == AD_ID_FIELD:
                    result["ad_id"] = str(value)
                
                # Campo 764700: JSON del pedido con "ad"
                elif field_id == ORDER_JSON_FIELD:
                    # Si aún no tenemos ad_id, intentar extraer del JSON
                    if not result["ad_id"] and isinstance(value, str) and value.startswith("{"):
                        try:
                            json_data = json.loads(value)
                            if json_data.get("ad"):
                                result["ad_id"] = str(json_data["ad"])
                            # También extraer otros campos del JSON si no los tenemos
                            if not result["total_a_pagar"] and json_data.get("total"):
                                try:
                                    result["total_a_pagar"] = float(json_data["total"])
                                except:
                                    pass
                        except json.JSONDecodeError:
                            pass
                
                # Campo 926799: Estado/Calificación
                elif field_id == ESTADO_FIELD:
                    result["calificacion"] = str(value)
                
                # Campo 117867: Total a pagar
                elif field_id == TOTAL_FIELD:
                    if not result["total_a_pagar"]:
                        try:
                            result["total_a_pagar"] = float(str(value).replace(",", "").replace("$", ""))
                        except:
                            pass
                
                # Campo 116501: Producto
                elif field_id == PRODUCTO_FIELD:
                    result["producto"] = str(value)[:500]
            
            return result
            
    except Exception as e:
        print(f"[CUSTOM FIELDS] Error fetching contact {contact_id}: {e}")
        return result


async def fetch_lucidbot_contacts_page(
    jwt_token: str,
    page_id: str,
    page: int = 0,
    page_size: int = 500,
    ad_id: str = None
) -> dict:
    """
    Obtener una página de contactos de LucidBot.
    Opcionalmente filtrar por ad_id.
    
    NOTA: Esta función solo trae campos básicos.
    Para obtener ad_id, usar fetch_contact_custom_fields() después.
    """
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"token={jwt_token}; last_page_id={page_id}"
    }
    
    # Construir condiciones de filtro
    cdts = []
    if ad_id:
        cdts = [{"col": "ad_id", "op": "=", "val": ad_id}]
    
    payload = {
        "op": "users",
        "op1": "get",
        "cdts": cdts,
        "oprt": 1,
        "search_text": "",
        "datatable": {
            "draw": page + 1,
            "start": page * page_size,
            "length": page_size,
            "orderByName": [{"column": {"name": "dt"}, "dir": "desc"}]
        },
        "pageName": "users",
        "page_id": page_id
    }
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            response = await client.post(
                LUCIDBOT_PHP_URL,
                headers=headers,
                json=payload
            )
            
            if response.status_code != 200:
                return {"success": False, "error": f"HTTP {response.status_code}"}
            
            data = response.json()
            
            if data.get("status") != "OK":
                return {"success": False, "error": "Token inválido o expirado"}
            
            return {
                "success": True,
                "contacts": data.get("data", []),
                "total": data.get("recordsTotal", 0)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}


async def enrich_contacts_with_ad_id(
    jwt_token: str,
    page_id: str,
    contacts: List[dict],
    batch_size: int = 10
) -> List[dict]:
    """
    Enriquecer lista de contactos con ad_id obtenido de custom_fields.
    
    Hace llamadas en paralelo (batch_size a la vez) para no sobrecargar la API.
    
    Args:
        jwt_token: Token de LucidBot
        page_id: ID de página
        contacts: Lista de contactos básicos
        batch_size: Cuántas llamadas en paralelo (default 10)
    
    Returns:
        Lista de contactos enriquecidos con ad_id
    """
    enriched = []
    total = len(contacts)
    
    for i in range(0, total, batch_size):
        batch = contacts[i:i + batch_size]
        
        # Crear tasks para obtener custom_fields en paralelo
        tasks = []
        for contact in batch:
            contact_id = contact.get("id") or contact.get("ph")
            if contact_id:
                tasks.append(fetch_contact_custom_fields(jwt_token, page_id, str(contact_id)))
            else:
                tasks.append(asyncio.coroutine(lambda: {})())
        
        # Ejecutar en paralelo
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Combinar resultados con contactos originales
        for j, contact in enumerate(batch):
            enriched_contact = contact.copy()
            
            if j < len(results) and isinstance(results[j], dict):
                cf_data = results[j]
                # Solo actualizar si tenemos valores
                if cf_data.get("ad_id"):
                    enriched_contact["ad_id"] = cf_data["ad_id"]
                if cf_data.get("total_a_pagar"):
                    enriched_contact["total_a_pagar_cf"] = cf_data["total_a_pagar"]
                if cf_data.get("producto"):
                    enriched_contact["producto_cf"] = cf_data["producto"]
                if cf_data.get("calificacion"):
                    enriched_contact["calificacion_cf"] = cf_data["calificacion"]
            
            enriched.append(enriched_contact)
        
        # Log progreso cada 50 contactos
        processed = min(i + batch_size, total)
        if processed % 50 == 0 or processed == total:
            with_ad_id = sum(1 for c in enriched if c.get("ad_id"))
            print(f"[ENRICH] {processed}/{total} procesados, {with_ad_id} con ad_id")
        
        # Pequeña pausa entre batches para no sobrecargar
        if i + batch_size < total:
            await asyncio.sleep(0.5)
    
    return enriched


async def fetch_all_contacts_for_ad(api_token: str, ad_id: str, page_id: str = None) -> List[dict]:
    """
    Obtener TODOS los contactos para un ad_id específico.
    Pagina automáticamente hasta obtener todos.
    
    Esta función es usada por analytics.py para auto-sync.
    """
    all_contacts = []
    page = 0
    page_size = 500
    
    # Si no hay page_id, intentar obtenerlo del token (no es posible, retornar vacío)
    if not page_id:
        print(f"[FETCH AD] No page_id provided for ad_id={ad_id}")
        return []
    
    while True:
        result = await fetch_lucidbot_contacts_page(
            jwt_token=api_token,
            page_id=page_id,
            page=page,
            page_size=page_size,
            ad_id=ad_id
        )
        
        if not result.get("success"):
            print(f"[FETCH AD] Error fetching page {page}: {result.get('error')}")
            break
        
        contacts = result.get("contacts", [])
        if not contacts:
            break
        
        all_contacts.extend(contacts)
        print(f"[FETCH AD] ad_id={ad_id} page {page}: {len(contacts)} contacts")
        
        if len(contacts) < page_size:
            break
        
        page += 1
        
        # Límite de seguridad
        if page > 100:
            break
    
    return all_contacts


def sync_contacts_to_db(db: Session, user_id: int, contacts: List[dict], ad_id: str = None) -> int:
    """
    Sincronizar una lista de contactos a la base de datos.
    Usa UPSERT para evitar duplicados.
    
    NOTA: Esta función es SÍNCRONA, no async.
    
    Los contactos deben venir enriquecidos con ad_id desde enrich_contacts_with_ad_id()
    """
    synced = 0
    errors = 0
    with_ad_id = 0
    
    for contact in contacts:
        try:
            lucidbot_id = contact.get("id")
            if not lucidbot_id:
                continue
            
            # Parsear fecha
            created_str = contact.get("dt", "")
            contact_created = None
            if created_str:
                try:
                    contact_created = datetime.strptime(created_str, "%Y-%m-%d %H:%M:%S")
                except:
                    contact_created = datetime.now()
            else:
                contact_created = datetime.now()
            
            # Extraer total_a_pagar y producto
            # Prioridad: custom_fields enriquecidos > campos cf básicos
            total_a_pagar = contact.get("total_a_pagar_cf")
            producto = contact.get("producto_cf")
            calificacion = contact.get("calificacion_cf")
            
            # Fallback a campos cf básicos si no hay datos enriquecidos
            if total_a_pagar is None or producto is None:
                campos = contact.get("cf", {})
                if isinstance(campos, dict):
                    for key, value in campos.items():
                        key_lower = key.lower()
                        if total_a_pagar is None and "total" in key_lower and "pagar" in key_lower:
                            try:
                                total_a_pagar = float(str(value).replace(",", "").replace("$", ""))
                            except:
                                pass
                        if producto is None and ("producto" in key_lower or "product" in key_lower):
                            producto = str(value)[:500] if value else None
            
            # ad_id: prioridad al enriquecido, fallback al parámetro
            contact_ad_id = contact.get("ad_id") or ad_id
            
            if contact_ad_id:
                with_ad_id += 1
            
            contact_data = {
                "user_id": user_id,
                "lucidbot_id": lucidbot_id,
                "full_name": contact.get("name", "") or contact.get("n", "") or "",
                "phone": contact.get("phone", "") or contact.get("ph", "") or "",
                "ad_id": contact_ad_id,
                "total_a_pagar": total_a_pagar,
                "producto": producto,
                "calificacion": calificacion or contact.get("qualification"),
                "contact_created_at": contact_created,
                "synced_at": datetime.utcnow(),
            }
            
            # UPSERT usando índice compuesto (user_id, lucidbot_id)
            stmt = pg_insert(LucidbotContact).values(**contact_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['user_id', 'lucidbot_id'],
                set_={
                    "full_name": stmt.excluded.full_name,
                    "phone": stmt.excluded.phone,
                    "ad_id": stmt.excluded.ad_id,
                    "total_a_pagar": stmt.excluded.total_a_pagar,
                    "producto": stmt.excluded.producto,
                    "calificacion": stmt.excluded.calificacion,
                    "synced_at": stmt.excluded.synced_at,
                    "updated_at": datetime.utcnow()
                }
            )
            db.execute(stmt)
            synced += 1
            
            # Commit cada 100 contactos para evitar transacciones muy largas
            if synced % 100 == 0:
                db.commit()
            
        except Exception as e:
            errors += 1
            db.rollback()
            if errors <= 3:
                print(f"[SYNC TO DB] Error processing contact: {e}")
            continue
    
    # Commit final
    try:
        db.commit()
    except Exception as e:
        print(f"[SYNC TO DB] Error in final commit: {e}")
        db.rollback()
    
    print(f"[SYNC TO DB] Completed: {synced} synced, {with_ad_id} with ad_id, {errors} errors")
    return synced


async def sync_contacts_for_user(
    user_id: int,
    jwt_token: str,
    page_id: str,
    db: Session = None
) -> dict:
    """
    Sincronizar TODOS los contactos de un usuario.
    
    FLUJO:
    1. Obtener lista de contactos (paginado)
    2. Enriquecer cada contacto con ad_id desde custom_fields
    3. Guardar en BD
    """
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True
    
    try:
        total_synced = 0
        total_with_ad_id = 0
        page = 0
        page_size = 500
        
        print(f"[LUCIDBOT SYNC] Starting sync for user {user_id}")
        
        while True:
            # PASO 1: Obtener página de contactos
            result = await fetch_lucidbot_contacts_page(jwt_token, page_id, page, page_size)
            
            if not result.get("success"):
                print(f"[LUCIDBOT SYNC] Error: {result.get('error')}")
                break
            
            contacts = result.get("contacts", [])
            if not contacts:
                break
            
            print(f"[LUCIDBOT SYNC] Page {page}: {len(contacts)} contacts, enriching with ad_id...")
            
            # PASO 2: Enriquecer contactos con ad_id
            enriched_contacts = await enrich_contacts_with_ad_id(
                jwt_token, 
                page_id, 
                contacts,
                batch_size=10  # 10 llamadas en paralelo
            )
            
            # Contar cuántos tienen ad_id
            page_with_ad_id = sum(1 for c in enriched_contacts if c.get("ad_id"))
            total_with_ad_id += page_with_ad_id
            
            print(f"[LUCIDBOT SYNC] Page {page}: {page_with_ad_id}/{len(contacts)} with ad_id")
            
            # PASO 3: Guardar en BD
            synced = sync_contacts_to_db(db, user_id, enriched_contacts)
            total_synced += synced
            
            if len(contacts) < page_size:
                break
            
            page += 1
            
            # Límite de seguridad: máximo 200 páginas (100,000 contactos)
            if page >= 200:
                print(f"[LUCIDBOT SYNC] Reached page limit, stopping")
                break
        
        print(f"[LUCIDBOT SYNC] Completed: {total_synced} contacts synced, {total_with_ad_id} with ad_id")
        return {
            "success": True, 
            "synced": total_synced,
            "with_ad_id": total_with_ad_id
        }
        
    except Exception as e:
        print(f"[LUCIDBOT SYNC] Error: {e}")
        return {"success": False, "error": str(e)}
    
    finally:
        if close_db:
            db.close()


async def sync_contacts_background(user_id: int, jwt_token: str, page_id: str):
    """
    Wrapper para ejecutar sync en background task.
    """
    print(f"[LUCIDBOT SYNC BG] Starting background sync for user {user_id}")
    result = await sync_contacts_for_user(user_id, jwt_token, page_id)
    print(f"[LUCIDBOT SYNC BG] Completed: {result}")
    return result


async def sync_all_lucidbot_users() -> list:
    """
    Sincronizar todos los usuarios con LucidBot conectado.
    Llamar esto desde el scheduler.
    """
    db = SessionLocal()
    try:
        connections = db.query(LucidbotConnection).filter(
            LucidbotConnection.is_active == True,
            LucidbotConnection.jwt_token_encrypted != None
        ).all()
        
        print(f"[LUCIDBOT CRON] Found {len(connections)} active LucidBot connections")
        
        results = []
        for conn in connections:
            user = db.query(User).filter(User.id == conn.user_id).first()
            if user and conn.jwt_token_encrypted:
                try:
                    jwt_token = decrypt_token(conn.jwt_token_encrypted)
                    print(f"[LUCIDBOT CRON] Syncing user {user.email}...")
                    
                    # Crear nueva sesión para cada usuario
                    user_db = SessionLocal()
                    try:
                        result = await sync_contacts_for_user(
                            conn.user_id, 
                            jwt_token, 
                            conn.page_id,
                            user_db
                        )
                    finally:
                        user_db.close()
                    
                    results.append({
                        "user_id": conn.user_id,
                        "email": user.email,
                        "result": result
                    })
                except Exception as e:
                    print(f"[LUCIDBOT CRON] Error syncing user {user.email}: {e}")
                    results.append({
                        "user_id": conn.user_id,
                        "email": user.email,
                        "result": {"success": False, "error": str(e)}
                    })
        
        return results
    finally:
        db.close()


# Router para endpoints manuales
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from database import get_db
from routers.auth import get_current_user

router = APIRouter()


@router.post("/lucidbot")
async def trigger_lucidbot_sync(
    background_tasks: BackgroundTasks,
    current_user = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Disparar sincronización manual de LucidBot"""
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not connection or not connection.jwt_token_encrypted:
        raise HTTPException(status_code=400, detail="No hay conexión de LucidBot configurada")
    
    jwt_token = decrypt_token(connection.jwt_token_encrypted)
    background_tasks.add_task(sync_contacts_background, current_user.id, jwt_token, connection.page_id)
    
    return {"message": "Sincronización iniciada", "status": "syncing"}


@router.get("/lucidbot/status")
async def get_sync_status(
    current_user = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Ver estado del último sync de LucidBot"""
    # Contar contactos totales y con ad_id
    total = db.query(LucidbotContact).filter(
        LucidbotContact.user_id == current_user.id
    ).count()
    
    with_ad_id = db.query(LucidbotContact).filter(
        LucidbotContact.user_id == current_user.id,
        LucidbotContact.ad_id != None
    ).count()
    
    # Último sync
    last_sync = db.query(LucidbotContact.synced_at).filter(
        LucidbotContact.user_id == current_user.id
    ).order_by(LucidbotContact.synced_at.desc()).first()
    
    return {
        "total_contacts": total,
        "with_ad_id": with_ad_id,
        "without_ad_id": total - with_ad_id,
        "ad_id_percentage": round(with_ad_id / total * 100, 1) if total > 0 else 0,
        "last_sync": last_sync[0].isoformat() if last_sync and last_sync[0] else None
    }
