"""
Router de Sincronización v2 - Usando endpoint user.php con JWT Token

Este módulo sincroniza contactos desde LucidBot usando el endpoint interno
user.php que permite paginación real (no el endpoint find_by_custom_field
que tiene paginación rota).

IMPORTANTE: Requiere JWT token de la cookie de sesión de LucidBot.
"""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func, and_
from typing import Optional
from datetime import datetime, timedelta
import httpx
import logging
import json

from database import get_db, User, LucidbotConnection, LucidbotContact
from routers.auth import get_current_user
from utils import encrypt_token, decrypt_token

router = APIRouter()
logger = logging.getLogger(__name__)

LUCIDBOT_PHP_URL = "https://panel.lucidbot.co/php/user.php"


# ========== JWT TOKEN MANAGEMENT ==========

@router.get("/lucidbot/jwt-status")
async def get_jwt_status(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Verificar estado del JWT token de LucidBot"""
    
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not connection:
        return {
            "has_jwt": False,
            "has_api_token": False,
            "page_id": None,
            "message": "No hay conexión con LucidBot"
        }
    
    has_jwt = bool(connection.jwt_token_encrypted)
    jwt_expires = None
    jwt_valid = False
    
    if has_jwt:
        try:
            jwt_token = decrypt_token(connection.jwt_token_encrypted)
            # Decodificar JWT para ver expiración (sin verificar firma)
            import base64
            parts = jwt_token.split('.')
            if len(parts) >= 2:
                payload = parts[1]
                # Agregar padding si es necesario
                payload += '=' * (4 - len(payload) % 4)
                decoded = json.loads(base64.b64decode(payload))
                expire_timestamp = decoded.get('expire', 0)
                jwt_expires = datetime.fromtimestamp(expire_timestamp)
                jwt_valid = jwt_expires > datetime.now()
        except Exception as e:
            logger.error(f"Error decodificando JWT: {e}")
    
    return {
        "has_jwt": has_jwt,
        "jwt_valid": jwt_valid,
        "jwt_expires": jwt_expires.isoformat() if jwt_expires else None,
        "has_api_token": bool(connection.api_token_encrypted),
        "page_id": connection.page_id,
        "message": "JWT válido" if jwt_valid else ("JWT expirado" if has_jwt else "Sin JWT token")
    }


@router.post("/lucidbot/jwt-token")
async def save_jwt_token(
    data: dict,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Guardar JWT token de LucidBot"""
    
    jwt_token = data.get("jwt_token", "").strip()
    page_id = data.get("page_id", "").strip()
    
    if not jwt_token:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="JWT token es requerido"
        )
    
    if not page_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Page ID es requerido"
        )
    
    # Validar formato JWT
    if not jwt_token.startswith("eyJ"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Token no tiene formato JWT válido"
        )
    
    # Verificar que el token funciona
    try:
        test_response = await test_jwt_token(jwt_token, page_id)
        if not test_response.get("success"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Token inválido o expirado: {test_response.get('error', 'Error desconocido')}"
            )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error verificando token: {str(e)}"
        )
    
    # Buscar o crear conexión
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id
    ).first()
    
    if not connection:
        connection = LucidbotConnection(
            user_id=current_user.id,
            page_id=page_id,
            is_active=True
        )
        db.add(connection)
    
    connection.jwt_token_encrypted = encrypt_token(jwt_token)
    connection.page_id = page_id
    connection.is_active = True
    connection.updated_at = datetime.utcnow()
    
    db.commit()
    
    return {
        "success": True,
        "message": "JWT token guardado correctamente",
        "total_contacts": test_response.get("total_contacts", 0)
    }


@router.delete("/lucidbot/jwt-token")
async def delete_jwt_token(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Eliminar JWT token"""
    
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id
    ).first()
    
    if connection:
        connection.jwt_token_encrypted = None
        db.commit()
    
    return {"success": True, "message": "JWT token eliminado"}


async def test_jwt_token(jwt_token: str, page_id: str) -> dict:
    """Probar si el JWT token funciona"""
    
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"token={jwt_token}; last_page_id={page_id}"
    }
    
    payload = {
        "op": "users",
        "op1": "get",
        "cdts": [],
        "oprt": 1,
        "search_text": "",
        "datatable": {
            "draw": 1,
            "start": 0,
            "length": 1,
            "orderByName": [{"column": {"name": "dt"}, "dir": "desc"}]
        },
        "pageName": "users",
        "page_id": page_id
    }
    
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                LUCIDBOT_PHP_URL,
                headers=headers,
                json=payload
            )
            
            if response.status_code != 200:
                return {"success": False, "error": f"HTTP {response.status_code}"}
            
            data = response.json()
            
            if data.get("status") != "OK":
                return {"success": False, "error": "Respuesta no OK"}
            
            return {
                "success": True,
                "total_contacts": data.get("recordsTotal", 0)
            }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ========== SYNC ENDPOINTS ==========

@router.post("/lucidbot/sync-all")
async def sync_all_contacts(
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Sincronizar TODOS los contactos de LucidBot.
    Usa el endpoint user.php con paginación real.
    """
    
    connection = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if not connection or not connection.jwt_token_encrypted:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Primero configura el JWT token de LucidBot en Configuración"
        )
    
    jwt_token = decrypt_token(connection.jwt_token_encrypted)
    page_id = connection.page_id
    
    # Ejecutar sync en background
    background_tasks.add_task(
        sync_contacts_background,
        current_user.id,
        jwt_token,
        page_id
    )
    
    return {
        "success": True,
        "message": "Sincronización iniciada en segundo plano",
        "status": "processing"
    }


@router.get("/lucidbot/sync-status")
async def get_sync_status(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener estado de la última sincronización"""
    
    # Contar contactos en BD
    total_contacts = db.query(func.count(LucidbotContact.id)).filter(
        LucidbotContact.user_id == current_user.id
    ).scalar()
    
    total_ventas = db.query(func.count(LucidbotContact.id)).filter(
        LucidbotContact.user_id == current_user.id,
        LucidbotContact.total_a_pagar > 0
    ).scalar()
    
    # Obtener último contacto sincronizado
    last_contact = db.query(LucidbotContact).filter(
        LucidbotContact.user_id == current_user.id
    ).order_by(LucidbotContact.synced_at.desc()).first()
    
    # Contactos por fecha
    contacts_by_date = db.query(
        func.date(LucidbotContact.contact_created_at).label('date'),
        func.count(LucidbotContact.id).label('total'),
        func.sum(
            func.case((LucidbotContact.total_a_pagar > 0, 1), else_=0)
        ).label('ventas')
    ).filter(
        LucidbotContact.user_id == current_user.id
    ).group_by(
        func.date(LucidbotContact.contact_created_at)
    ).order_by(
        func.date(LucidbotContact.contact_created_at).desc()
    ).limit(30).all()
    
    # Ads únicos
    unique_ads = db.query(
        LucidbotContact.ad_id,
        func.count(LucidbotContact.id).label('contacts'),
        func.sum(
            func.case((LucidbotContact.total_a_pagar > 0, 1), else_=0)
        ).label('ventas')
    ).filter(
        LucidbotContact.user_id == current_user.id,
        LucidbotContact.ad_id != None,
        LucidbotContact.ad_id != ""
    ).group_by(LucidbotContact.ad_id).all()
    
    return {
        "total_contacts": total_contacts,
        "total_ventas": total_ventas,
        "total_ads": len(unique_ads),
        "last_sync": last_contact.synced_at.isoformat() if last_contact else None,
        "by_date": [
            {
                "date": str(row.date),
                "total": row.total,
                "ventas": int(row.ventas or 0)
            }
            for row in contacts_by_date
        ],
        "by_ad": [
            {
                "ad_id": row.ad_id,
                "contacts": row.contacts,
                "ventas": int(row.ventas or 0)
            }
            for row in unique_ads[:50]  # Limitar a 50 ads
        ]
    }


async def sync_contacts_background(user_id: int, jwt_token: str, page_id: str):
    """
    Función de sincronización que corre en background.
    Descarga TODOS los contactos usando paginación real.
    Luego obtiene custom_fields de cada contacto para extraer ad_id.
    """
    from database import SessionLocal
    
    db = SessionLocal()
    
    try:
        logger.info(f"[SYNC V2] Iniciando sincronización para user_id={user_id}")
        
        headers = {
            "Content-Type": "application/json",
            "Cookie": f"token={jwt_token}; last_page_id={page_id}"
        }
        
        # Primero obtener total de registros
        total_records = 0
        page_size = 200  # Máximo permitido
        start = 0
        all_contacts = []
        seen_ids = set()
        
        async with httpx.AsyncClient(timeout=60) as client:
            # Primera llamada para saber el total
            payload = {
                "op": "users",
                "op1": "get",
                "cdts": [],
                "oprt": 1,
                "search_text": "",
                "datatable": {
                    "draw": 1,
                    "start": 0,
                    "length": 1,
                    "orderByName": [{"column": {"name": "dt"}, "dir": "desc"}]
                },
                "pageName": "users",
                "page_id": page_id
            }
            
            response = await client.post(LUCIDBOT_PHP_URL, headers=headers, json=payload)
            
            if response.status_code != 200:
                logger.error(f"[SYNC V2] Error HTTP: {response.status_code}")
                return
            
            data = response.json()
            total_records = data.get("recordsTotal", 0)
            logger.info(f"[SYNC V2] Total de contactos en LucidBot: {total_records}")
            
            # Ahora paginar todos
            while start < total_records:
                payload["datatable"]["start"] = start
                payload["datatable"]["length"] = page_size
                payload["datatable"]["draw"] = (start // page_size) + 1
                
                response = await client.post(LUCIDBOT_PHP_URL, headers=headers, json=payload)
                
                if response.status_code != 200:
                    logger.error(f"[SYNC V2] Error en página {start}: HTTP {response.status_code}")
                    break
                
                data = response.json()
                contacts = data.get("data", [])
                
                if not contacts:
                    logger.info(f"[SYNC V2] No más contactos en página {start}")
                    break
                
                for contact in contacts:
                    contact_id = contact.get("id")
                    if contact_id and contact_id not in seen_ids:
                        seen_ids.add(contact_id)
                        all_contacts.append(contact)
                
                logger.info(f"[SYNC V2] Página {start//page_size + 1}: {len(contacts)} contactos, total únicos: {len(all_contacts)}")
                
                start += page_size
        
        logger.info(f"[SYNC V2] Descargados {len(all_contacts)} contactos únicos. Obteniendo custom_fields...")
        
        # Guardar en BD con custom_fields
        await save_contacts_with_details(db, user_id, all_contacts, jwt_token, page_id)
        
        logger.info(f"[SYNC V2] Sincronización completada para user_id={user_id}")
        
    except Exception as e:
        logger.error(f"[SYNC V2] Error en sincronización: {str(e)}")
    finally:
        db.close()


async def save_contacts_with_details(db: Session, user_id: int, contacts: list, jwt_token: str, page_id: str):
    """
    Guardar contactos en la base de datos, obteniendo custom_fields de cada uno.
    
    Custom Fields importantes (IDs específicos de tu cuenta):
    - 728462: Anuncio Facebook (ad_id)
    - 926799: Estado/Calificación
    - 117867: Total a pagar
    - 116501: Producto
    """
    
    # Limpiar contactos existentes del usuario
    deleted = db.query(LucidbotContact).filter(
        LucidbotContact.user_id == user_id
    ).delete()
    db.commit()
    logger.info(f"[SYNC V2] Eliminados {deleted} contactos previos")
    
    headers = {
        "Content-Type": "application/json",
        "Cookie": f"token={jwt_token}; last_page_id={page_id}"
    }
    
    created = 0
    errors = 0
    
    # Mapping de field_ids conocidos (pueden variar por cuenta)
    # Estos son genéricos, el sistema los detectará automáticamente
    FIELD_MAPPINGS = {
        "ad_id_keywords": ["anuncio", "facebook", "ad_id", "ad"],
        "producto_keywords": ["producto", "product"],
        "total_keywords": ["total", "pagar", "valor", "price"],
        "estado_keywords": ["estado", "calificacion", "status", "qualification"]
    }
    
    async with httpx.AsyncClient(timeout=30) as client:
        for i, contact in enumerate(contacts):
            try:
                ms_id = contact.get("id")
                
                # Obtener custom_fields del contacto
                detail_payload = [{
                    "op": "users",
                    "op1": "get",
                    "ms_id": ms_id,
                    "expand": {"boards": True},
                    "pageName": "inbox",
                    "page_id": page_id
                }]
                
                ad_id = ""
                producto = ""
                total_a_pagar = 0.0
                calificacion = ""
                
                try:
                    detail_response = await client.post(
                        LUCIDBOT_PHP_URL,
                        headers=headers,
                        json=detail_payload,
                        timeout=15
                    )
                    
                    if detail_response.status_code == 200:
                        detail_data = detail_response.json()
                        
                        if detail_data and len(detail_data) > 0:
                            user_data = detail_data[0].get("data", {})
                            custom_fields = user_data.get("custom_fields", [])
                            
                            # Primero intentar obtener del field_id conocido (728462 = Anuncio Facebook)
                            for cf in custom_fields:
                                field_id = cf.get("id", "")
                                value = cf.get("value", "")
                                
                                if not value:
                                    continue
                                
                                # Field ID específico para ad_id
                                if field_id == "728462":
                                    ad_id = str(value)
                                elif field_id == "926799":  # Estado/Calificación
                                    calificacion = str(value)
                                elif field_id == "117867":  # Total a pagar
                                    try:
                                        total_a_pagar = float(str(value).replace(",", "").replace("$", ""))
                                    except:
                                        pass
                                elif field_id == "116501":  # Producto
                                    producto = str(value)
                            
                            # Si no encontramos ad_id por ID específico, buscar en campo 764700 (JSON completo)
                            if not ad_id:
                                for cf in custom_fields:
                                    value = cf.get("value", "")
                                    if value and isinstance(value, str) and value.startswith("{"):
                                        try:
                                            json_data = json.loads(value)
                                            if "ad" in json_data:
                                                ad_id = str(json_data["ad"])
                                            if not producto and "products" in json_data:
                                                producto = str(json_data.get("notes", ""))
                                            if total_a_pagar == 0 and "total" in json_data:
                                                total_a_pagar = float(json_data["total"])
                                        except:
                                            pass
                
                except Exception as e:
                    logger.debug(f"[SYNC V2] Error obteniendo detalles de {ms_id}: {e}")
                
                # Parsear fecha
                created_at = None
                created_at_str = contact.get("dt")
                if created_at_str:
                    try:
                        created_at = datetime.strptime(created_at_str, "%Y-%m-%d %H:%M:%S")
                    except:
                        pass
                
                # Guardar contacto
                db_contact = LucidbotContact(
                    user_id=user_id,
                    lucidbot_id=int(ms_id) if ms_id else 0,
                    phone=contact.get("ph", ""),
                    full_name=contact.get("n", ""),
                    ad_id=ad_id,
                    contact_created_at=created_at,
                    calificacion=calificacion,
                    producto=producto,
                    total_a_pagar=total_a_pagar,
                    raw_data=json.dumps(contact),
                    synced_at=datetime.utcnow()
                )
                
                db.add(db_contact)
                created += 1
                
                # Commit cada 50 contactos y log progreso
                if created % 50 == 0:
                    db.commit()
                    logger.info(f"[SYNC V2] Procesados {created}/{len(contacts)} contactos...")
                
            except Exception as e:
                errors += 1
                logger.error(f"[SYNC V2] Error guardando contacto: {str(e)}")
                continue
    
    db.commit()
    
    # Stats finales
    with_ad = db.query(func.count(LucidbotContact.id)).filter(
        LucidbotContact.user_id == user_id,
        LucidbotContact.ad_id != "",
        LucidbotContact.ad_id != None
    ).scalar()
    
    with_sale = db.query(func.count(LucidbotContact.id)).filter(
        LucidbotContact.user_id == user_id,
        LucidbotContact.total_a_pagar > 0
    ).scalar()
    
    logger.info(f"[SYNC V2] ✅ Sincronización completada:")
    logger.info(f"[SYNC V2]    - Total contactos: {created}")
    logger.info(f"[SYNC V2]    - Con ad_id: {with_ad}")
    logger.info(f"[SYNC V2]    - Con venta: {with_sale}")
    logger.info(f"[SYNC V2]    - Errores: {errors}")


# ========== OBTENER CONTACTOS DE UN AD ==========

@router.get("/lucidbot/contacts-by-ad/{ad_id}")
async def get_contacts_by_ad(
    ad_id: str,
    start_date: str,
    end_date: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener contactos de un ad_id específico desde BD local"""
    
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    
    contacts = db.query(LucidbotContact).filter(
        and_(
            LucidbotContact.user_id == current_user.id,
            LucidbotContact.ad_id == ad_id,
            LucidbotContact.contact_created_at >= start_dt,
            LucidbotContact.contact_created_at <= end_dt
        )
    ).all()
    
    leads = 0
    sales = 0
    revenue = 0.0
    
    for c in contacts:
        if c.total_a_pagar and c.total_a_pagar > 0:
            sales += 1
            revenue += c.total_a_pagar
        else:
            leads += 1
    
    return {
        "ad_id": ad_id,
        "date_range": {"start": start_date, "end": end_date},
        "leads": leads,
        "sales": sales,
        "revenue": revenue,
        "total_contacts": len(contacts)
    }
