"""
Sincronización de contactos de LucidBot
Traemos TODOS los contactos por página y los guardamos localmente.
"""

import httpx
from datetime import datetime, timedelta
from typing import Optional, List
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import text
import json

from database import SessionLocal, LucidbotConnection, LucidbotContact, User
from utils import decrypt_token

LUCIDBOT_PHP_URL = "https://panel.lucidbot.co/php/user.php"


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
    """
    synced = 0
    errors = 0
    
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
            total_a_pagar = None
            producto = None
            
            campos = contact.get("cf", {})
            if isinstance(campos, dict):
                for key, value in campos.items():
                    key_lower = key.lower()
                    if "total" in key_lower and "pagar" in key_lower:
                        try:
                            total_a_pagar = float(str(value).replace(",", "").replace("$", ""))
                        except:
                            pass
                    if "producto" in key_lower or "product" in key_lower:
                        producto = str(value)[:500] if value else None
            
            contact_data = {
                "user_id": user_id,
                "lucidbot_id": lucidbot_id,
                "full_name": contact.get("name", "") or contact.get("n", "") or "",
                "phone": contact.get("phone", "") or contact.get("ph", "") or "",
                "ad_id": contact.get("ad_id") or ad_id,
                "total_a_pagar": total_a_pagar,
                "producto": producto,
                "calificacion": contact.get("qualification"),
                "contact_created_at": contact_created,
                "synced_at": datetime.utcnow(),
            }
            
            # UPSERT usando índice compuesto (user_id, lucidbot_id)
            stmt = pg_insert(LucidbotContact).values(**contact_data)
            stmt = stmt.on_conflict_do_update(
                index_elements=['user_id', 'lucidbot_id'],  # CAMBIADO: usar índice compuesto
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
            # Rollback la transacción actual para poder continuar
            db.rollback()
            if errors <= 3:  # Solo mostrar primeros 3 errores
                print(f"[SYNC TO DB] Error processing contact: {e}")
            continue
    
    # Commit final
    try:
        db.commit()
    except Exception as e:
        print(f"[SYNC TO DB] Error in final commit: {e}")
        db.rollback()
    
    print(f"[SYNC TO DB] Completed: {synced} synced, {errors} errors")
    return synced


async def sync_contacts_for_user(
    user_id: int,
    jwt_token: str,
    page_id: str,
    db: Session = None
) -> dict:
    """
    Sincronizar TODOS los contactos de un usuario.
    """
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True
    
    try:
        total_synced = 0
        page = 0
        page_size = 500
        
        print(f"[LUCIDBOT SYNC] Starting sync for user {user_id}")
        
        while True:
            result = await fetch_lucidbot_contacts_page(jwt_token, page_id, page, page_size)
            
            if not result.get("success"):
                print(f"[LUCIDBOT SYNC] Error: {result.get('error')}")
                break
            
            contacts = result.get("contacts", [])
            if not contacts:
                break
            
            print(f"[LUCIDBOT SYNC] Processing page {page} with {len(contacts)} contacts")
            
            # sync_contacts_to_db es síncrono
            synced = sync_contacts_to_db(db, user_id, contacts)
            total_synced += synced
            
            if len(contacts) < page_size:
                break
            
            page += 1
            
            # Límite de seguridad: máximo 200 páginas (100,000 contactos)
            if page >= 200:
                print(f"[LUCIDBOT SYNC] Reached page limit, stopping")
                break
        
        print(f"[LUCIDBOT SYNC] Completed: {total_synced} contacts synced")
        return {"success": True, "synced": total_synced}
        
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
