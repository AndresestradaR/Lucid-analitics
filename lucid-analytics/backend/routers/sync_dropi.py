"""
Sincronización de Dropi - Orders y Wallet History
Similar a sync.py de LucidBot pero para Dropi

Flujo:
1. sync_dropi_orders() - Sincroniza todos los pedidos
2. sync_dropi_wallet() - Sincroniza historial de wallet
3. reconcile_orders_wallet() - Cruza datos para marcar pagos/cobros
"""

import httpx
import json
import asyncio
from datetime import datetime, timedelta
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy import text

from database import SessionLocal, DropiConnection, DropiOrder, DropiWalletHistory, User
from utils import decrypt_token

# URLs por país
DROPI_API_URLS = {
    "gt": "https://api.dropi.gt",
    "co": "https://api.dropi.co",
    "mx": "https://api.dropi.mx",
    "cl": "https://api.dropi.cl",
    "pe": "https://api.dropi.pe",
    "ec": "https://api.dropi.ec",
}

WHITE_BRAND_IDS = {
    "gt": 1,
    "co": "df3e6b0bb66ceaadca4f84cbc371fd66e04d20fe51fc414da8d1b84d31d178de",
    "mx": "df3e6b0bb66ceaadca4f84cbc371fd66e04d20fe51fc414da8d1b84d31d178de",
    "cl": "df3e6b0bb66ceaadca4f84cbc371fd66e04d20fe51fc414da8d1b84d31d178de",
    "pe": "df3e6b0bb66ceaadca4f84cbc371fd66e04d20fe51fc414da8d1b84d31d178de",
    "ec": "df3e6b0bb66ceaadca4f84cbc371fd66e04d20fe51fc414da8d1b84d31d178de",
}

# Estados normalizados
STATUS_NORMALIZE = {
    "ENTREGADO": "ENTREGADO",
    "DEVOLUCION": "DEVOLUCION",
    "DEVOLUCIÓN": "DEVOLUCION",
    "CANCELADO": "CANCELADO",
    "PENDIENTE": "PENDIENTE",
    "PENDIENTE CONFIRMACION": "PENDIENTE_CONFIRMACION",
    "PENDIENTE CONFIRMACIÓN": "PENDIENTE_CONFIRMACION",
    "NOVEDAD": "EN_RUTA",
    "EN CAMINO": "EN_RUTA",
    "ENVIADO": "EN_RUTA",
    "EN REPARTO": "EN_RUTA",
    "EN BODEGA": "EN_RUTA",
    "GUÍA GENERADA": "EN_RUTA",
    "GUIA GENERADA": "EN_RUTA",
    "EN DESPACHO": "EN_RUTA",
    "RECIBIDO": "EN_RUTA",
    "EN TERMINAL": "EN_RUTA",
    "EN TRÁNSITO": "EN_RUTA",
    "EN TRANSITO": "EN_RUTA",
}


def get_dropi_headers(token: str = None, country: str = "co"):
    """
    Headers COMPLETOS para requests a Dropi - CRÍTICO para evitar "Access denied"
    Estos headers imitan exactamente a un navegador Chrome real.
    """
    origin = f"https://app.dropi.{country}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": origin,
        "Referer": f"{origin}/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site"
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


async def dropi_login(email: str, password: str, country: str) -> dict:
    """Hacer login en Dropi y obtener token con timeout real"""
    api_url = DROPI_API_URLS.get(country, DROPI_API_URLS["co"])
    white_brand_id = WHITE_BRAND_IDS.get(country, WHITE_BRAND_IDS["co"])
    
    payload = {
        "email": email,
        "password": password,
        "white_brand_id": white_brand_id,
        "brand": "",
        "otp": None,
        "with_cdc": False
    }
    
    try:
        async with asyncio.timeout(20):  # Timeout real de 20 segundos
            async with httpx.AsyncClient(timeout=httpx.Timeout(15.0, connect=5.0)) as client:
                response = await client.post(
                    f"{api_url}/api/login",
                    json=payload,
                    headers=get_dropi_headers(country=country)
                )
                data = response.json()
                
                if data.get("isSuccess") and data.get("token"):
                    user_data = data.get("objects", {})
                    return {
                        "success": True,
                        "token": data["token"],
                        "user_id": str(user_data.get("id", "")),
                    }
                return {"success": False, "error": data.get("message", "Login failed")}
    except asyncio.TimeoutError:
        return {"success": False, "error": "Dropi no responde (timeout)"}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def fetch_dropi_orders(token: str, country: str, page: int = 0, limit: int = 100) -> dict:
    """
    Obtener órdenes de Dropi con paginación.
    Dropi usa 'start' para offset y 'result_number' para limit.
    """
    api_url = DROPI_API_URLS.get(country, DROPI_API_URLS["co"])
    
    params = {
        "result_number": limit,
        "start": page * limit,
        "order_by": "updated_at",  # Ordenar por última actualización para sync incremental
        "order_dir": "desc"
    }
    
    try:
        async with asyncio.timeout(60):  # Timeout de 60s para órdenes
            async with httpx.AsyncClient(timeout=httpx.Timeout(55.0, connect=10.0)) as client:
                response = await client.get(
                    f"{api_url}/api/orders/myorders",
                    headers=get_dropi_headers(token, country),
                    params=params
                )
                
                if response.status_code == 401:
                    return {"success": False, "error": "Token expirado", "expired": True}
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("isSuccess"):
                        return {
                            "success": True,
                            "orders": data.get("objects", []),
                            "total": data.get("total", 0)
                        }
                
                return {"success": False, "error": f"HTTP {response.status_code}"}
    except asyncio.TimeoutError:
        return {"success": False, "error": "Timeout fetching orders"}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def fetch_dropi_wallet(token: str, country: str, user_id: str, page: int = 0, limit: int = 500, from_date: str = None) -> dict:
    """
    Obtener historial de wallet con paginación.
    """
    api_url = DROPI_API_URLS.get(country, DROPI_API_URLS["co"])
    
    # Si no hay from_date, usar hace 2 años
    if not from_date:
        from_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
    
    params = {
        "orderBy": "id",
        "orderDirection": "desc",
        "result_number": limit,
        "start": page * limit,
        "textToSearch": "",
        "type": "null",
        "id": "null",
        "identification_code": "null",
        "user_id": user_id,
        "from": from_date,
        "until": datetime.now().strftime("%Y-%m-%d"),
        "wallet_id": 0
    }
    
    try:
        async with asyncio.timeout(60):
            async with httpx.AsyncClient(timeout=httpx.Timeout(55.0, connect=10.0)) as client:
                response = await client.get(
                    f"{api_url}/api/historywallet",
                    headers=get_dropi_headers(token, country),
                    params=params
                )
                
                if response.status_code == 401:
                    return {"success": False, "error": "Token expirado", "expired": True}
                
                if response.status_code == 200:
                    data = response.json()
                    if data.get("isSuccess"):
                        return {
                            "success": True,
                            "movements": data.get("objects", []),
                            "total": data.get("total", 0)
                        }
                
                return {"success": False, "error": f"HTTP {response.status_code}"}
    except asyncio.TimeoutError:
        return {"success": False, "error": "Timeout fetching wallet"}
    except Exception as e:
        return {"success": False, "error": str(e)}


def normalize_status(status_raw: str) -> str:
    """Normalizar status de Dropi a categorías estándar"""
    if not status_raw:
        return "DESCONOCIDO"
    status_upper = str(status_raw).upper().strip()
    return STATUS_NORMALIZE.get(status_upper, "EN_RUTA")


def categorize_wallet_movement(description: str, movement_type: str) -> str:
    """Categorizar movimiento de wallet"""
    desc_upper = (description or "").upper()
    
    if "ENTRADA POR GANANCIA EN LA ORDEN COMO DROPSHIPPER" in desc_upper:
        return "ganancia_dropshipping"
    elif "SALIDA POR COBRO DE FLETE INICIAL" in desc_upper:
        return "cobro_flete"
    elif "RETIRO" in desc_upper:
        return "retiro"
    elif "RECARGA" in desc_upper or "DEPOSITO" in desc_upper:
        return "recarga"
    elif movement_type == "ENTRADA":
        return "entrada_otro"
    elif movement_type == "SALIDA":
        return "salida_otro"
    return "otro"


async def sync_dropi_orders_for_user(
    user_id: int,
    token: str,
    country: str,
    db: Session,
    full_sync: bool = False
) -> dict:
    """
    Sincronizar órdenes de Dropi para un usuario.
    
    - full_sync=True: Trae TODO el histórico (primera vez)
    - full_sync=False: Solo órdenes actualizadas en los últimos 7 días
    """
    print(f"[DROPI SYNC] Starting orders sync for user {user_id}, full_sync={full_sync}")
    
    total_synced = 0
    total_errors = 0
    page = 0
    limit = 100
    
    # Para sync incremental, traemos solo las últimas 500 órdenes
    max_orders = 10000 if full_sync else 500
    
    while total_synced < max_orders:
        result = await fetch_dropi_orders(token, country, page, limit)
        
        if not result.get("success"):
            print(f"[DROPI SYNC] Error fetching orders page {page}: {result.get('error')}")
            if result.get("expired"):
                return {"success": False, "error": "Token expirado", "synced": total_synced}
            break
        
        orders = result.get("orders", [])
        if not orders:
            print(f"[DROPI SYNC] No more orders at page {page}")
            break
        
        print(f"[DROPI SYNC] Processing page {page} with {len(orders)} orders")
        
        for order in orders:
            try:
                dropi_order_id = order.get("id")
                if not dropi_order_id:
                    continue
                
                # Parsear fechas
                created_str = order.get("created_at", "")
                updated_str = order.get("updated_at", "")
                
                order_created = None
                order_updated = None
                
                if created_str:
                    try:
                        order_created = datetime.strptime(created_str[:19], "%Y-%m-%dT%H:%M:%S")
                    except:
                        pass
                
                if updated_str:
                    try:
                        order_updated = datetime.strptime(updated_str[:19], "%Y-%m-%dT%H:%M:%S")
                    except:
                        pass
                
                if not order_created:
                    continue
                
                # Normalizar status - puede venir como string o como objeto
                status_raw = order.get("status", "")
                if isinstance(status_raw, dict):
                    status_raw = status_raw.get("name", status_raw.get("id", ""))
                status_raw = str(status_raw).strip()
                status_normalized = normalize_status(status_raw)
                
                # Extraer productos
                products = []
                for detail in order.get("orderdetails", []):
                    product = detail.get("product", {})
                    products.append({
                        "name": product.get("name", "Producto"),
                        "quantity": detail.get("quantity", 1),
                        "price": float(detail.get("price", 0))
                    })
                
                # Preparar datos para UPSERT
                order_data = {
                    "user_id": user_id,
                    "dropi_order_id": dropi_order_id,
                    "status": status_normalized,
                    "status_raw": status_raw,
                    "total_order": float(order.get("total_order", 0)),
                    "shipping_amount": float(order.get("shipping_amount", 0)),
                    "dropshipper_profit": float(order.get("dropshipper_amount_to_win", 0)),
                    "customer_name": f"{order.get('name', '')} {order.get('surname', '')}".strip(),
                    "customer_phone": order.get("phone"),
                    "customer_city": order.get("city"),
                    "customer_state": order.get("state"),
                    "customer_address": order.get("dir"),
                    "shipping_guide": order.get("shipping_guide"),
                    "shipping_company": order.get("shipping_company"),
                    "rate_type": order.get("rate_type"),
                    "products_json": json.dumps(products) if products else None,
                    "order_created_at": order_created,
                    "order_updated_at": order_updated,
                    "synced_at": datetime.utcnow(),
                    "raw_data": json.dumps(order)
                }
                
                # UPSERT usando PostgreSQL ON CONFLICT
                stmt = pg_insert(DropiOrder).values(**order_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['user_id', 'dropi_order_id'],
                    set_={
                        "status": stmt.excluded.status,
                        "status_raw": stmt.excluded.status_raw,
                        "total_order": stmt.excluded.total_order,
                        "shipping_amount": stmt.excluded.shipping_amount,
                        "dropshipper_profit": stmt.excluded.dropshipper_profit,
                        "customer_name": stmt.excluded.customer_name,
                        "customer_phone": stmt.excluded.customer_phone,
                        "shipping_guide": stmt.excluded.shipping_guide,
                        "order_updated_at": stmt.excluded.order_updated_at,
                        "synced_at": stmt.excluded.synced_at,
                        "raw_data": stmt.excluded.raw_data,
                        "updated_at": datetime.utcnow()
                    }
                )
                db.execute(stmt)
                total_synced += 1
                
                # Commit cada 50 órdenes para evitar transacciones muy largas
                if total_synced % 50 == 0:
                    db.commit()
                
            except Exception as e:
                total_errors += 1
                db.rollback()  # IMPORTANTE: Rollback para poder continuar
                if total_errors <= 3:
                    print(f"[DROPI SYNC] Error processing order {order.get('id')}: {e}")
                continue
        
        # Commit al final de cada página
        try:
            db.commit()
        except Exception as e:
            print(f"[DROPI SYNC] Error committing page {page}: {e}")
            db.rollback()
        
        # Si recibimos menos órdenes que el límite, ya no hay más
        if len(orders) < limit:
            break
        
        page += 1
    
    print(f"[DROPI SYNC] Orders sync completed: {total_synced} orders synced, {total_errors} errors")
    return {"success": True, "synced": total_synced, "errors": total_errors}


async def sync_dropi_wallet_for_user(
    user_id: int,
    token: str,
    country: str,
    dropi_user_id: str,
    db: Session,
    full_sync: bool = False
) -> dict:
    """
    Sincronizar historial de wallet de Dropi para un usuario.
    """
    print(f"[DROPI SYNC] Starting wallet sync for user {user_id}")
    
    total_synced = 0
    total_errors = 0
    page = 0
    limit = 500
    
    # Para full sync, traer desde hace 2 años; para incremental, 60 días
    from_date = (datetime.now() - timedelta(days=730 if full_sync else 60)).strftime("%Y-%m-%d")
    
    max_movements = 5000 if full_sync else 1000
    
    while total_synced < max_movements:
        result = await fetch_dropi_wallet(token, country, dropi_user_id, page, limit, from_date)
        
        if not result.get("success"):
            print(f"[DROPI SYNC] Error fetching wallet page {page}: {result.get('error')}")
            break
        
        movements = result.get("movements", [])
        if not movements:
            break
        
        print(f"[DROPI SYNC] Processing wallet page {page} with {len(movements)} movements")
        
        for mov in movements:
            try:
                dropi_wallet_id = mov.get("id")
                if not dropi_wallet_id:
                    continue
                
                created_str = mov.get("created_at", "")
                movement_created = None
                if created_str:
                    try:
                        movement_created = datetime.strptime(created_str[:19], "%Y-%m-%dT%H:%M:%S")
                    except:
                        pass
                
                if not movement_created:
                    continue
                
                movement_type = mov.get("type", "")
                description = mov.get("description", "")
                
                wallet_data = {
                    "user_id": user_id,
                    "dropi_wallet_id": dropi_wallet_id,
                    "movement_type": movement_type,
                    "description": description,
                    "amount": abs(float(mov.get("amount", 0))),
                    "balance_after": float(mov.get("previous_amount", 0)),  # Balance después
                    "order_id": mov.get("order_id"),
                    "category": categorize_wallet_movement(description, movement_type),
                    "movement_created_at": movement_created,
                    "synced_at": datetime.utcnow(),
                    "raw_data": json.dumps(mov)
                }
                
                # UPSERT
                stmt = pg_insert(DropiWalletHistory).values(**wallet_data)
                stmt = stmt.on_conflict_do_update(
                    index_elements=['user_id', 'dropi_wallet_id'],
                    set_={
                        "movement_type": stmt.excluded.movement_type,
                        "description": stmt.excluded.description,
                        "amount": stmt.excluded.amount,
                        "balance_after": stmt.excluded.balance_after,
                        "order_id": stmt.excluded.order_id,
                        "category": stmt.excluded.category,
                        "synced_at": stmt.excluded.synced_at,
                    }
                )
                db.execute(stmt)
                total_synced += 1
                
                # Commit cada 100 movimientos
                if total_synced % 100 == 0:
                    db.commit()
                
            except Exception as e:
                total_errors += 1
                db.rollback()  # IMPORTANTE: Rollback para poder continuar
                if total_errors <= 3:
                    print(f"[DROPI SYNC] Error processing wallet movement {mov.get('id')}: {e}")
                continue
        
        try:
            db.commit()
        except Exception as e:
            print(f"[DROPI SYNC] Error committing wallet page {page}: {e}")
            db.rollback()
        
        if len(movements) < limit:
            break
        
        page += 1
    
    print(f"[DROPI SYNC] Wallet sync completed: {total_synced} movements synced, {total_errors} errors")
    return {"success": True, "synced": total_synced, "errors": total_errors}


async def reconcile_orders_wallet(user_id: int, db: Session) -> dict:
    """
    Cruzar órdenes con wallet para marcar cuáles ya fueron pagadas/cobradas.
    Esto permite saber exactamente qué ganancias ya están en tu wallet.
    """
    print(f"[DROPI SYNC] Starting reconciliation for user {user_id}")
    
    try:
        # 1. Obtener todos los movimientos de ganancia con order_id
        ganancias = db.query(DropiWalletHistory).filter(
            DropiWalletHistory.user_id == user_id,
            DropiWalletHistory.category == "ganancia_dropshipping",
            DropiWalletHistory.order_id != None
        ).all()
        
        # 2. Obtener todos los cobros de flete con order_id
        cobros = db.query(DropiWalletHistory).filter(
            DropiWalletHistory.user_id == user_id,
            DropiWalletHistory.category == "cobro_flete",
            DropiWalletHistory.order_id != None
        ).all()
        
        # 3. Crear mapas order_id -> wallet_movement
        pagos_map = {g.order_id: g for g in ganancias}
        cobros_map = {c.order_id: c for c in cobros}
        
        print(f"[DROPI SYNC] Found {len(pagos_map)} payments, {len(cobros_map)} charges")
        
        # 4. Actualizar órdenes con info de pago
        updated_paid = 0
        updated_charged = 0
        
        # Marcar órdenes pagadas
        for order_id, wallet_mov in pagos_map.items():
            try:
                result = db.execute(
                    text("""
                        UPDATE dropi_orders 
                        SET is_paid = true, 
                            paid_at = :paid_at, 
                            paid_amount = :amount,
                            wallet_transaction_id = :wallet_id,
                            updated_at = :now
                        WHERE user_id = :user_id AND dropi_order_id = :order_id AND is_paid = false
                    """),
                    {
                        "paid_at": wallet_mov.movement_created_at,
                        "amount": wallet_mov.amount,
                        "wallet_id": wallet_mov.dropi_wallet_id,
                        "now": datetime.utcnow(),
                        "user_id": user_id,
                        "order_id": order_id
                    }
                )
                updated_paid += result.rowcount
            except Exception as e:
                db.rollback()
                continue
        
        db.commit()
        
        # Marcar órdenes con devolución cobrada
        for order_id, wallet_mov in cobros_map.items():
            try:
                result = db.execute(
                    text("""
                        UPDATE dropi_orders 
                        SET is_return_charged = true, 
                            return_charged_at = :charged_at, 
                            return_charged_amount = :amount,
                            updated_at = :now
                        WHERE user_id = :user_id AND dropi_order_id = :order_id AND is_return_charged = false
                    """),
                    {
                        "charged_at": wallet_mov.movement_created_at,
                        "amount": wallet_mov.amount,
                        "now": datetime.utcnow(),
                        "user_id": user_id,
                        "order_id": order_id
                    }
                )
                updated_charged += result.rowcount
            except Exception as e:
                db.rollback()
                continue
        
        db.commit()
        
        print(f"[DROPI SYNC] Reconciliation completed: {updated_paid} paid, {updated_charged} charged")
        return {"success": True, "updated_paid": updated_paid, "updated_charged": updated_charged}
        
    except Exception as e:
        print(f"[DROPI SYNC] Reconciliation error: {e}")
        db.rollback()
        return {"success": False, "error": str(e)}


async def sync_dropi_full(user_id: int, db: Session = None) -> dict:
    """
    Sincronización completa de Dropi para un usuario.
    Esta es la función principal que se llama desde el admin o cron.
    """
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True
    
    connection = None
    
    try:
        # 1. Obtener conexión del usuario
        connection = db.query(DropiConnection).filter(
            DropiConnection.user_id == user_id,
            DropiConnection.is_active == True
        ).first()
        
        if not connection:
            return {"success": False, "error": "No hay conexión de Dropi"}
        
        # 2. Marcar como sincronizando
        connection.sync_status = "syncing"
        db.commit()
        
        # 3. Hacer login para obtener token fresco
        email = decrypt_token(connection.email_encrypted)
        password = decrypt_token(connection.password_encrypted)
        
        print(f"[DROPI SYNC] Logging in for user {user_id}...")
        login_result = await dropi_login(email, password, connection.country)
        if not login_result.get("success"):
            connection.sync_status = "error"
            db.commit()
            return {"success": False, "error": f"Login failed: {login_result.get('error')}"}
        
        token = login_result["token"]
        dropi_user_id = login_result.get("user_id") or connection.dropi_user_id
        
        print(f"[DROPI SYNC] Login successful, dropi_user_id={dropi_user_id}")
        
        # Actualizar token
        connection.current_token = token
        connection.token_expires_at = datetime.utcnow() + timedelta(hours=24)
        db.commit()
        
        # 4. Determinar si es full sync (primera vez) o incremental
        is_full_sync = connection.last_orders_sync is None
        
        # 5. Sincronizar órdenes
        orders_result = await sync_dropi_orders_for_user(
            user_id=user_id,
            token=token,
            country=connection.country,
            db=db,
            full_sync=is_full_sync
        )
        
        # 6. Sincronizar wallet
        wallet_result = await sync_dropi_wallet_for_user(
            user_id=user_id,
            token=token,
            country=connection.country,
            dropi_user_id=dropi_user_id,
            db=db,
            full_sync=is_full_sync
        )
        
        # 7. Reconciliar datos
        reconcile_result = await reconcile_orders_wallet(user_id, db)
        
        # 8. Actualizar estado
        connection.last_orders_sync = datetime.utcnow()
        connection.last_wallet_sync = datetime.utcnow()
        connection.sync_status = "completed"
        db.commit()
        
        return {
            "success": True,
            "orders_synced": orders_result.get("synced", 0),
            "wallet_synced": wallet_result.get("synced", 0),
            "reconciled_paid": reconcile_result.get("updated_paid", 0),
            "reconciled_charged": reconcile_result.get("updated_charged", 0)
        }
        
    except Exception as e:
        print(f"[DROPI SYNC] Error: {e}")
        if connection:
            try:
                connection.sync_status = "error"
                db.commit()
            except:
                db.rollback()
        return {"success": False, "error": str(e)}
    
    finally:
        if close_db:
            db.close()


# ========== FUNCIÓN PARA BACKGROUND TASK ==========

async def sync_dropi_background(user_id: int):
    """
    Wrapper para ejecutar sync en background task.
    """
    print(f"[DROPI SYNC BG] Starting background sync for user {user_id}")
    result = await sync_dropi_full(user_id)
    print(f"[DROPI SYNC BG] Completed: {result}")
    return result


# ========== CRON JOB ==========

async def sync_all_dropi_users():
    """
    Sincronizar todos los usuarios con Dropi conectado.
    Llamar esto desde un cron cada 1-2 horas.
    """
    db = SessionLocal()
    try:
        connections = db.query(DropiConnection).filter(
            DropiConnection.is_active == True
        ).all()
        
        print(f"[DROPI CRON] Found {len(connections)} active Dropi connections")
        
        results = []
        for conn in connections:
            user = db.query(User).filter(User.id == conn.user_id).first()
            if user:
                print(f"[DROPI CRON] Syncing user {user.email}...")
                
                # Crear nueva sesión para cada usuario para evitar problemas de transacción
                user_db = SessionLocal()
                try:
                    result = await sync_dropi_full(conn.user_id, user_db)
                finally:
                    user_db.close()
                
                results.append({
                    "user_id": conn.user_id,
                    "email": user.email,
                    "result": result
                })
        
        return results
    finally:
        db.close()
