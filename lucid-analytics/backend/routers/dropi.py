"""
Router de Dropi
Maneja conexión y consultas a la API de Dropi (Colombia, Guatemala, etc.)
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import Optional
from datetime import datetime, timedelta
from pydantic import BaseModel
import httpx
import time
import asyncio

from database import get_db, User, DropiConnection
from routers.auth import get_current_user
from utils import encrypt_token, decrypt_token

router = APIRouter()

# URLs por país
DROPI_API_URLS = {
    "gt": "https://api.dropi.gt",
    "co": "https://api.dropi.co",
    "mx": "https://api.dropi.mx",
    "cl": "https://api.dropi.cl",
    "pe": "https://api.dropi.pe",
    "ec": "https://api.dropi.ec",
}

# white_brand_id por país (Dropi usa estos IDs para cada país)
# Según documentación oficial: siempre debe ser este hash para Colombia
WHITE_BRAND_IDS = {
    "gt": 1,
    "co": "df3e6b0bb66ceaadca4f84cbc371fd66e04d20fe51fc414da8d1b84d31d178de",
    "mx": "df3e6b0bb66ceaadca4f84cbc371fd66e04d20fe51fc414da8d1b84d31d178de",
    "cl": "df3e6b0bb66ceaadca4f84cbc371fd66e04d20fe51fc414da8d1b84d31d178de",
    "pe": "df3e6b0bb66ceaadca4f84cbc371fd66e04d20fe51fc414da8d1b84d31d178de",
    "ec": "df3e6b0bb66ceaadca4f84cbc371fd66e04d20fe51fc414da8d1b84d31d178de",
}


# ========== SCHEMAS ==========

class DropiConnectRequest(BaseModel):
    email: str
    password: str
    country: str = "co"


class DropiConnectionResponse(BaseModel):
    id: int
    country: str
    dropi_user_id: Optional[str]
    dropi_user_name: Optional[str]
    is_active: bool
    created_at: datetime
    
    class Config:
        from_attributes = True


# ========== HELPERS ==========

def get_dropi_headers(token: str = None, country: str = "co"):
    """Headers para requests a Dropi - EXACTOS del MCP que funciona"""
    # Determinar origen según país
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
    """
    Hace login en Dropi y obtiene el token
    Con timeout REAL usando asyncio.timeout()
    """
    start_time = time.time()
    api_url = DROPI_API_URLS.get(country, DROPI_API_URLS["co"])
    white_brand_id = WHITE_BRAND_IDS.get(country, WHITE_BRAND_IDS["co"])
    
    url = f"{api_url}/api/login"
    
    # Payload EXACTO del MCP que funciona (con brand, otp, with_cdc)
    payload = {
        "email": email,
        "password": password,
        "white_brand_id": white_brand_id,
        "brand": "",
        "otp": None,
        "with_cdc": False
    }
    
    print(f"[DROPI LOGIN] START - {email}")
    
    try:
        # asyncio.timeout() es el ÚNICO que garantiza cortar en el tiempo especificado
        async with asyncio.timeout(15):  # 15 segundos MÁXIMO
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
                response = await client.post(
                    url, 
                    json=payload, 
                    headers=get_dropi_headers(country=country)
                )
                
                elapsed = time.time() - start_time
                print(f"[DROPI LOGIN] Response in {elapsed:.2f}s - Status: {response.status_code}")
                
                # Intentar parsear respuesta
                try:
                    data = response.json()
                except:
                    return {"success": False, "error": f"Respuesta inválida de Dropi"}
                
                # Verificar respuesta exitosa
                if data.get("isSuccess") and data.get("token"):
                    user_data = data.get("objects", {})
                    
                    # Extraer wallet del usuario - múltiples formatos posibles
                    wallet_balance = 0
                    
                    # Formato 1: wallet como objeto con amount
                    wallet_obj = user_data.get("wallet")
                    if isinstance(wallet_obj, dict):
                        wallet_balance = float(wallet_obj.get("amount", 0) or 0)
                    elif wallet_obj is not None:
                        try:
                            wallet_balance = float(wallet_obj)
                        except:
                            pass
                    
                    # Formato 2: wallets como array
                    if wallet_balance == 0:
                        wallets = user_data.get("wallets", [])
                        if wallets and isinstance(wallets, list):
                            for w in wallets:
                                if isinstance(w, dict) and w.get("amount"):
                                    wallet_balance = float(w.get("amount", 0))
                                    break
                    
                    # Formato 3: balance directo en user
                    if wallet_balance == 0:
                        balance = user_data.get("balance")
                        if balance:
                            try:
                                wallet_balance = float(balance)
                            except:
                                pass
                    
                    print(f"[DROPI LOGIN] SUCCESS - {email} - wallet: {wallet_balance} in {elapsed:.2f}s")
                    
                    return {
                        "success": True, 
                        "token": data["token"],
                        "user_id": str(user_data.get("id", "")),
                        "user_name": f"{user_data.get('name', '')} {user_data.get('surname', '')}".strip(),
                        "wallet_balance": wallet_balance
                    }
                else:
                    # Extraer mensaje de error
                    error_msg = data.get("message", "") or data.get("error", "") or "Login fallido"
                    print(f"[DROPI LOGIN] FAILED - {email} - {error_msg} in {elapsed:.2f}s")
                    return {"success": False, "error": error_msg}
                    
    except asyncio.TimeoutError:
        elapsed = time.time() - start_time
        print(f"[DROPI LOGIN] TIMEOUT after {elapsed:.2f}s - {email}")
        return {"success": False, "error": "Dropi no responde (timeout 15s)"}
    except httpx.TimeoutException:
        elapsed = time.time() - start_time
        print(f"[DROPI LOGIN] HTTP TIMEOUT after {elapsed:.2f}s - {email}")
        return {"success": False, "error": "Timeout conectando con Dropi"}
    except httpx.RequestError as e:
        elapsed = time.time() - start_time
        print(f"[DROPI LOGIN] CONNECTION ERROR after {elapsed:.2f}s - {email}")
        return {"success": False, "error": f"Error de conexión: {str(e)[:100]}"}
    except Exception as e:
        elapsed = time.time() - start_time
        print(f"[DROPI LOGIN] EXCEPTION after {elapsed:.2f}s - {email} - {type(e).__name__}")
        return {"success": False, "error": f"Error: {str(e)[:100]}"}


async def dropi_request(method: str, endpoint: str, token: str, country: str, params: dict = None, payload: dict = None) -> dict:
    """Request genérico a la API de Dropi con timeout real"""
    api_url = DROPI_API_URLS.get(country, DROPI_API_URLS["co"])
    url = f"{api_url}{endpoint}"
    
    try:
        # Timeout real de 30 segundos para requests de datos
        async with asyncio.timeout(30):
            async with httpx.AsyncClient(timeout=httpx.Timeout(25.0, connect=5.0)) as client:
                if method == "GET":
                    response = await client.get(url, headers=get_dropi_headers(token, country), params=params)
                else:
                    response = await client.post(url, headers=get_dropi_headers(token, country), json=payload)
                
                if response.status_code == 401:
                    return {"success": False, "error": "Token expirado", "expired": True}
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        if data.get("isSuccess") == True:
                            return {"success": True, "data": data}
                        else:
                            return {"success": False, "error": data.get("message", "API error")}
                    except Exception as e:
                        return {"success": False, "error": f"JSON parse error"}
                else:
                    return {"success": False, "error": f"HTTP {response.status_code}"}
                    
    except asyncio.TimeoutError:
        print(f"[DROPI REQUEST] TIMEOUT - {endpoint}")
        return {"success": False, "error": "Dropi no responde (timeout 30s)"}
    except Exception as e:
        print(f"[DROPI REQUEST] ERROR - {endpoint} - {type(e).__name__}")
        return {"success": False, "error": str(e)[:100]}


async def ensure_dropi_token(connection: DropiConnection, db: Session) -> str:
    """Asegura que hay un token válido, hace re-login si es necesario"""
    # Si hay token y no ha expirado, usarlo
    if connection.current_token and connection.token_expires_at:
        if connection.token_expires_at > datetime.utcnow():
            return connection.current_token
    
    # Re-login
    email = decrypt_token(connection.email_encrypted)
    password = decrypt_token(connection.password_encrypted)
    
    result = await dropi_login(email, password, connection.country)
    
    if not result.get("success"):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=f"Error re-autenticando con Dropi: {result.get('error')}"
        )
    
    # Guardar nuevo token (expira en 24h)
    connection.current_token = result["token"]
    connection.token_expires_at = datetime.utcnow() + timedelta(hours=24)
    db.commit()
    
    return result["token"]


# ========== ENDPOINTS ==========

@router.post("/connect", response_model=DropiConnectionResponse)
async def connect_dropi(
    data: DropiConnectRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Conectar cuenta de Dropi"""
    
    # Validar país
    if data.country not in DROPI_API_URLS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"País no soportado. Opciones: {', '.join(DROPI_API_URLS.keys())}"
        )
    
    # Intentar login
    result = await dropi_login(data.email, data.password, data.country)
    
    if not result.get("success"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error de autenticación: {result.get('error')}"
        )
    
    # Buscar conexión existente
    existing = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id
    ).first()
    
    if existing:
        # Actualizar
        existing.email_encrypted = encrypt_token(data.email)
        existing.password_encrypted = encrypt_token(data.password)
        existing.country = data.country
        existing.current_token = result["token"]
        existing.token_expires_at = datetime.utcnow() + timedelta(hours=24)
        existing.dropi_user_id = result.get("user_id")
        existing.dropi_user_name = result.get("user_name")
        existing.is_active = True
        existing.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(existing)
        return DropiConnectionResponse.model_validate(existing)
    
    # Crear nueva
    connection = DropiConnection(
        user_id=current_user.id,
        email_encrypted=encrypt_token(data.email),
        password_encrypted=encrypt_token(data.password),
        country=data.country,
        current_token=result["token"],
        token_expires_at=datetime.utcnow() + timedelta(hours=24),
        dropi_user_id=result.get("user_id"),
        dropi_user_name=result.get("user_name")
    )
    db.add(connection)
    db.commit()
    db.refresh(connection)
    
    return DropiConnectionResponse.model_validate(connection)


@router.get("/status")
async def get_dropi_status(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Estado de conexión de Dropi"""
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id,
        DropiConnection.is_active == True
    ).first()
    
    if not connection:
        return {"connected": False}
    
    return {
        "connected": True,
        "country": connection.country,
        "dropi_user_id": connection.dropi_user_id,
        "dropi_user_name": connection.dropi_user_name,
        "created_at": connection.created_at.isoformat()
    }


@router.post("/test-login")
async def test_dropi_login(data: DropiConnectRequest):
    """
    Endpoint de debug para probar login a Dropi SIN guardar nada.
    Útil para diagnosticar problemas de conexión.
    """
    print(f"[DROPI TEST] ========== Testing login for {data.email} ==========")
    start = time.time()
    
    result = await dropi_login(data.email, data.password, data.country)
    
    elapsed = time.time() - start
    print(f"[DROPI TEST] Completed in {elapsed:.2f}s - Success: {result.get('success')}")
    
    if result.get("success"):
        return {
            "success": True,
            "user_id": result.get("user_id"),
            "user_name": result.get("user_name"),
            "wallet_balance": result.get("wallet_balance"),
            "elapsed_seconds": round(elapsed, 2),
            "message": "Login exitoso - credenciales válidas"
        }
    else:
        return {
            "success": False,
            "error": result.get("error"),
            "elapsed_seconds": round(elapsed, 2),
            "message": f"Login falló: {result.get('error')}"
        }


@router.delete("/disconnect")
async def disconnect_dropi(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Desconectar Dropi"""
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id
    ).first()
    
    if connection:
        connection.is_active = False
        connection.current_token = None
        db.commit()
    
    return {"message": "Dropi desconectado"}


@router.get("/wallet")
async def get_wallet(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener saldo de la wallet"""
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id,
        DropiConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="No hay conexión de Dropi")
    
    token = await ensure_dropi_token(connection, db)
    
    # Obtener historial para calcular saldo
    result = await dropi_request(
        "GET", 
        "/api/historywallet", 
        token, 
        connection.country,
        params={"result_number": 1}
    )
    
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    
    data = result.get("data", {})
    records = data.get("objects", [])
    
    balance = 0
    if records and len(records) > 0:
        balance = float(records[0].get("balance", 0))
    
    return {
        "balance": balance,
        "currency": "COP" if connection.country == "co" else "GTQ",
        "country": connection.country
    }


@router.get("/wallet/history")
async def get_wallet_history(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days: int = 30,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Historial de movimientos de la wallet con datos diarios para gráficas"""
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id,
        DropiConnection.is_active == True
    ).first()
    
    if not connection:
        return {"movements": [], "summary": {"total_in": 0, "total_out": 0, "net": 0, "count": 0}, "daily": [], "period": {}}
    
    # Hacer login fresco para obtener token válido
    token = None
    user_id = None
    try:
        email = decrypt_token(connection.email_encrypted)
        password = decrypt_token(connection.password_encrypted)
        login_result = await dropi_login(email, password, connection.country)
        if login_result.get("success"):
            token = login_result.get("token")
            user_id = login_result.get("user_id")
            # Actualizar token en BD
            connection.current_token = token
            connection.token_expires_at = datetime.utcnow() + timedelta(hours=24)
            db.commit()
        else:
            error_msg = login_result.get('error', '')
            # Si credenciales incorrectas, desactivar conexión
            if "incorrecta" in error_msg.lower() or "denied" in error_msg.lower() or "bloqueo" in error_msg.lower():
                connection.is_active = False
                connection.current_token = None
                db.commit()
    except Exception as e:
        pass
    
    if not token:
        return {"movements": [], "summary": {"total_in": 0, "total_out": 0, "net": 0, "count": 0}, "daily": [], "period": {}}
    
    # Calcular fechas primero
    try:
        if start_date and end_date:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        else:
            end_dt = datetime.now()
            start_dt = end_dt - timedelta(days=days)
    except:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days)
    
    # Llamar a historywallet con parámetros EXACTOS del MCP que funciona
    result = await dropi_request(
        "GET",
        "/api/historywallet",
        token,
        connection.country,
        params={
            "orderBy": "id",
            "orderDirection": "desc",
            "result_number": 500,
            "start": 0,
            "textToSearch": "",
            "type": "null",
            "id": "null",
            "identification_code": "null",
            "user_id": user_id,
            "from": start_dt.strftime("%Y-%m-%d"),
            "until": end_dt.strftime("%Y-%m-%d"),
            "wallet_id": 0
        }
    )
    
    if not result.get("success"):
        return {"movements": [], "summary": {"total_in": 0, "total_out": 0, "net": 0, "count": 0}, "daily": [], "period": {}}
    
    data = result.get("data", {})
    records = data.get("objects", [])
    
    filtered = []
    total_in = 0
    total_out = 0
    
    # Contadores específicos para dropshipping
    total_ganancias = 0
    total_devoluciones = 0
    count_ganancias = 0
    count_devoluciones = 0
    
    # Diccionario para agrupar por día
    daily_data = {}
    daily_dropshipping = {}
    
    # Inicializar todos los días del período con 0
    try:
        current_day = start_dt
        while current_day <= end_dt:
            day_key = current_day.strftime("%Y-%m-%d")
            daily_data[day_key] = {"ingresos": 0, "egresos": 0, "date": day_key}
            daily_dropshipping[day_key] = {"ganancias": 0, "devoluciones": 0, "date": day_key}
            current_day += timedelta(days=1)
    except Exception:
        pass
    
    # Procesar registros
    for record in records:
        try:
            created_str = record.get("created_at", "")
            if not created_str:
                continue
            
            created_dt = datetime.strptime(created_str[:19], "%Y-%m-%dT%H:%M:%S")
            if not (start_dt <= created_dt <= end_dt):
                continue
            
            amount = abs(float(record.get("amount", 0) or 0))
            mov_type = record.get("type", "")
            description = record.get("description", "") or ""
            day_key = created_dt.strftime("%Y-%m-%d")
            
            # Usar el campo 'type' para determinar si es entrada o salida
            if mov_type == "ENTRADA":
                total_in += amount
                if day_key in daily_data:
                    daily_data[day_key]["ingresos"] += amount
                
                # Detectar ganancias de dropshipping
                if "ENTRADA POR GANANCIA EN LA ORDEN COMO DROPSHIPPER" in description:
                    total_ganancias += amount
                    count_ganancias += 1
                    if day_key in daily_dropshipping:
                        daily_dropshipping[day_key]["ganancias"] += amount
                        
            elif mov_type == "SALIDA":
                total_out += amount
                if day_key in daily_data:
                    daily_data[day_key]["egresos"] += amount
                
                # Detectar cobros de flete (devoluciones)
                if "SALIDA POR COBRO DE FLETE INICIAL" in description:
                    total_devoluciones += amount
                    count_devoluciones += 1
                    if day_key in daily_dropshipping:
                        daily_dropshipping[day_key]["devoluciones"] += amount
            
            filtered.append({
                "id": record.get("id"),
                "amount": amount,
                "balance": float(record.get("previous_amount", 0) or 0),
                "description": description,
                "type": mov_type,
                "order_id": record.get("order_id"),
                "created_at": created_str
            })
        except Exception:
            continue
    
    # Convertir daily_data a lista ordenada por fecha
    daily_list = sorted(daily_data.values(), key=lambda x: x["date"])
    daily_drop_list = sorted(daily_dropshipping.values(), key=lambda x: x["date"])
    
    # Formatear fechas para mostrar en gráfica (DD/MM)
    for item in daily_list:
        try:
            date_obj = datetime.strptime(item["date"], "%Y-%m-%d")
            item["display_date"] = date_obj.strftime("%d/%m")
        except:
            item["display_date"] = item["date"]
    
    for item in daily_drop_list:
        try:
            date_obj = datetime.strptime(item["date"], "%Y-%m-%d")
            item["display_date"] = date_obj.strftime("%d/%m")
        except:
            item["display_date"] = item["date"]
    
    # Calcular promedios
    promedio_ganancia = round(total_ganancias / count_ganancias, 2) if count_ganancias > 0 else 0
    promedio_devolucion = round(total_devoluciones / count_devoluciones, 2) if count_devoluciones > 0 else 0
    
    return {
        "movements": filtered[:100],
        "summary": {
            "total_in": total_in,
            "total_out": total_out,
            "net": total_in - total_out,
            "count": len(filtered)
        },
        "daily": daily_list,
        "dropshipping": {
            "total_ganancias": total_ganancias,
            "total_devoluciones": total_devoluciones,
            "utilidad_neta": total_ganancias - total_devoluciones,
            "count_ganancias": count_ganancias,
            "count_devoluciones": count_devoluciones,
            "promedio_ganancia": promedio_ganancia,
            "promedio_devolucion": promedio_devolucion,
            "daily": daily_drop_list
        },
        "period": {
            "start": start_dt.strftime("%Y-%m-%d"),
            "end": end_dt.strftime("%Y-%m-%d")
        }
    }


async def _fetch_orders_stats(token: str, country: str, start_date: str = None, end_date: str = None, days: int = 7):
    """Función auxiliar para obtener estadísticas de órdenes (no es endpoint)"""
    
    # Calcular fechas
    if start_date and end_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    else:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days)
    
    # Obtener órdenes
    result = await dropi_request(
        "GET",
        "/api/orders/myorders",
        token,
        country,
        params={
            "result_number": 500,
            "order_by": "created_at",
            "order_dir": "desc"
        }
    )
    
    if not result.get("success"):
        return {
            "stats": {
                "total": 0, "pending_confirmation": 0, "en_ruta": 0,
                "delivered": 0, "returned": 0, "cancelled": 0,
                "total_sales": 0, "delivered_profit": 0, "pending_profit": 0,
                "return_cost": 0, "net_profit": 0, "delivery_rate": 0,
                "effective_delivery_rate": 0
            },
            "period": {"start": start_dt.strftime("%Y-%m-%d"), "end": end_dt.strftime("%Y-%m-%d")},
            "error": result.get("error")
        }
    
    data = result.get("data", {})
    orders = data.get("objects", [])
    
    # Categorización de estados para proyección de negocio
    DELIVERED_STATES = {"ENTREGADO"}
    RETURNED_STATES = {"DEVOLUCION", "DEVOLUCIÓN"}
    CANCELLED_STATES = {"CANCELADO"}
    PENDING_CONFIRMATION_STATES = {"PENDIENTE", "PENDIENTE CONFIRMACION", "PENDIENTE CONFIRMACIÓN"}
    
    # Contadores
    stats = {
        "total": 0,
        "pending_confirmation": 0,
        "en_ruta": 0,
        "delivered": 0,
        "returned": 0,
        "cancelled": 0,
        "total_sales": 0,
        "delivered_profit": 0,
        "pending_profit": 0,
        "return_cost": 0,
    }
    
    daily_data = {}
    
    for order in orders:
        created_str = order.get("created_at", "")
        if not created_str:
            continue
        
        try:
            created_dt = datetime.strptime(created_str[:19], "%Y-%m-%dT%H:%M:%S")
        except:
            continue
        
        if not (start_dt <= created_dt <= end_dt):
            continue
        
        # Obtener status
        status_raw = order.get("status", "")
        if isinstance(status_raw, dict):
            status_raw = status_raw.get("name", status_raw.get("id", "unknown"))
        
        status_upper = str(status_raw).upper().strip()
        
        total_order = float(order.get("total_order", 0))
        profit = float(order.get("dropshipper_amount_to_win", 0))
        
        stats["total"] += 1
        stats["total_sales"] += total_order
        
        # Categorizar según lógica de negocio
        if status_upper in DELIVERED_STATES:
            stats["delivered"] += 1
            stats["delivered_profit"] += profit
        elif status_upper in RETURNED_STATES:
            stats["returned"] += 1
            stats["return_cost"] += 23000
        elif status_upper in CANCELLED_STATES:
            stats["cancelled"] += 1
        elif status_upper in PENDING_CONFIRMATION_STATES:
            stats["pending_confirmation"] += 1
        else:
            stats["en_ruta"] += 1
            stats["pending_profit"] += profit
        
        # Agregar a datos diarios para gráfico
        day_key = created_dt.strftime("%Y-%m-%d")
        if day_key not in daily_data:
            daily_data[day_key] = {
                "date": day_key, 
                "delivered": 0, 
                "returned": 0, 
                "en_ruta": 0, 
                "total": 0,
                "ganancias": 0,
                "devoluciones": 0
            }
        daily_data[day_key]["total"] += 1
        if status_upper in DELIVERED_STATES:
            daily_data[day_key]["delivered"] += 1
            daily_data[day_key]["ganancias"] += profit
        elif status_upper in RETURNED_STATES:
            daily_data[day_key]["returned"] += 1
            daily_data[day_key]["devoluciones"] += 23000
        elif status_upper not in CANCELLED_STATES and status_upper not in PENDING_CONFIRMATION_STATES:
            daily_data[day_key]["en_ruta"] += 1
    
    # Convertir daily_data a lista ordenada por fecha
    daily_list = sorted(daily_data.values(), key=lambda x: x["date"])
    
    # Calcular métricas
    stats["net_profit"] = stats["delivered_profit"] - stats["return_cost"]
    stats["delivery_rate"] = round((stats["delivered"] / stats["total"] * 100) if stats["total"] > 0 else 0, 1)
    
    completed = stats["delivered"] + stats["returned"]
    stats["effective_delivery_rate"] = round((stats["delivered"] / completed * 100) if completed > 0 else 0, 1)
    stats["effective_return_rate"] = round((stats["returned"] / completed * 100) if completed > 0 else 0, 1)
    stats["total_operativo"] = stats["delivered"] + stats["returned"] + stats["en_ruta"]
    stats["cancellation_rate"] = round((stats["cancelled"] / stats["total"] * 100) if stats["total"] > 0 else 0, 1)
    stats["completion_rate"] = round((completed / stats["total_operativo"] * 100) if stats["total_operativo"] > 0 else 0, 1)
    
    return {
        "stats": stats,
        "period": {"start": start_dt.strftime("%Y-%m-%d"), "end": end_dt.strftime("%Y-%m-%d")},
        "daily": daily_list
    }


@router.get("/orders")
async def get_orders(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days: int = 7,
    status_filter: Optional[str] = None,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener órdenes de Dropi con métricas"""
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id,
        DropiConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="No hay conexión de Dropi")
    
    token = await ensure_dropi_token(connection, db)
    
    # Calcular fechas
    if start_date and end_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    else:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days)
    
    # Obtener órdenes
    result = await dropi_request(
        "GET",
        "/api/orders/myorders",
        token,
        connection.country,
        params={
            "result_number": 500,
            "order_by": "created_at",
            "order_dir": "desc"
        }
    )
    
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    
    data = result.get("data", {})
    orders = data.get("objects", [])
    
    # Mapeo de estados
    STATUS_MAP = {
        1: "pending",
        2: "confirmed",
        3: "shipped",
        4: "delivered",
        5: "returned",
        6: "cancelled",
    }
    
    # Contadores y métricas
    stats = {
        "total": 0,
        "pending": 0,
        "confirmed": 0,
        "shipped": 0,
        "delivered": 0,
        "returned": 0,
        "cancelled": 0,
        "total_sales": 0,
        "delivered_profit": 0,
        "pending_profit": 0,
        "return_cost": 0,
    }
    
    filtered_orders = []
    
    for order in orders:
        created_str = order.get("created_at", "")
        if not created_str:
            continue
        
        try:
            created_dt = datetime.strptime(created_str[:19], "%Y-%m-%dT%H:%M:%S")
        except:
            continue
        
        if not (start_dt <= created_dt <= end_dt):
            continue
        
        status_id = order.get("status", 1)
        status_name = STATUS_MAP.get(status_id, "unknown")
        
        # Aplicar filtro de estado si existe
        if status_filter and status_name != status_filter:
            continue
        
        total_order = float(order.get("total_order", 0))
        profit = float(order.get("dropshipper_amount_to_win", 0))
        
        stats["total"] += 1
        stats["total_sales"] += total_order
        
        if status_name in stats:
            stats[status_name] += 1
        
        if status_name == "delivered":
            stats["delivered_profit"] += profit
        elif status_name in ["pending", "confirmed", "shipped"]:
            stats["pending_profit"] += profit
        elif status_name == "returned":
            stats["return_cost"] += 23000
        
        filtered_orders.append({
            "id": order.get("id"),
            "status": status_name,
            "status_id": status_id,
            "customer": f"{order.get('name', '')} {order.get('surname', '')}".strip(),
            "phone": order.get("phone"),
            "city": order.get("city"),
            "total": total_order,
            "profit": profit,
            "shipping_guide": order.get("shipping_guide"),
            "created_at": created_str
        })
    
    # Calcular profit neto
    stats["net_profit"] = stats["delivered_profit"] - stats["return_cost"]
    stats["delivery_rate"] = round(
        (stats["delivered"] / stats["total"] * 100) if stats["total"] > 0 else 0, 1
    )
    
    return {
        "orders": filtered_orders[:100],
        "stats": stats,
        "period": {
            "start": start_dt.strftime("%Y-%m-%d"),
            "end": end_dt.strftime("%Y-%m-%d")
        }
    }


@router.get("/orders/{order_id}")
async def get_order_detail(
    order_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Detalle de una orden específica"""
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id,
        DropiConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="No hay conexión de Dropi")
    
    token = await ensure_dropi_token(connection, db)
    
    result = await dropi_request(
        "GET",
        f"/api/orders/{order_id}",
        token,
        connection.country
    )
    
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    
    data = result.get("data", {})
    order = data.get("objects", {})
    
    if not order:
        raise HTTPException(status_code=404, detail="Orden no encontrada")
    
    return {
        "id": order.get("id"),
        "status": order.get("status"),
        "customer": {
            "name": f"{order.get('name', '')} {order.get('surname', '')}".strip(),
            "phone": order.get("phone"),
            "email": order.get("client_email"),
            "address": order.get("dir"),
            "city": order.get("city"),
            "state": order.get("state")
        },
        "financials": {
            "total": float(order.get("total_order", 0)),
            "shipping": float(order.get("shipping_amount", 0)),
            "profit": float(order.get("dropshipper_amount_to_win", 0))
        },
        "shipping": {
            "guide": order.get("shipping_guide"),
            "company": order.get("shipping_company"),
            "type": order.get("rate_type")
        },
        "products": [
            {
                "name": d.get("product", {}).get("name", "Producto"),
                "quantity": d.get("quantity", 1),
                "price": float(d.get("price", 0))
            }
            for d in order.get("orderdetails", [])
        ],
        "created_at": order.get("created_at"),
        "updated_at": order.get("updated_at")
    }


@router.get("/summary")
async def get_dropi_summary(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days: int = 7,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Resumen completo de Dropi para el Dashboard con cruce de datos"""
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id,
        DropiConnection.is_active == True
    ).first()
    
    if not connection:
        return {
            "connected": False,
            "message": "Conecta tu cuenta de Dropi para ver métricas"
        }
    
    # Obtener wallet Y token fresco haciendo login
    wallet_balance = 0
    token = None
    user_id = None
    login_failed = False
    login_error_msg = None
    
    try:
        email = decrypt_token(connection.email_encrypted)
        password = decrypt_token(connection.password_encrypted)
        
        # Hacer login fresco para obtener wallet Y token actualizado
        login_result = await dropi_login(email, password, connection.country)
        if login_result.get("success"):
            wallet_balance = login_result.get("wallet_balance", 0)
            token = login_result.get("token")
            user_id = login_result.get("user_id")
            
            # Actualizar token en BD para futuras peticiones
            connection.current_token = token
            connection.token_expires_at = datetime.utcnow() + timedelta(hours=24)
            db.commit()
        else:
            login_failed = True
            login_error_msg = login_result.get("error", "Login failed")
            
            # Si las credenciales son incorrectas, desactivar conexión y retornar INMEDIATAMENTE
            if "incorrecta" in login_error_msg.lower() or "denied" in login_error_msg.lower() or "bloqueo" in login_error_msg.lower():
                connection.is_active = False
                connection.current_token = None
                db.commit()
                return {
                    "connected": False,
                    "error": f"Credenciales de Dropi inválidas. Reconecta tu cuenta.",
                    "needs_reconnect": True
                }
    except Exception as e:
        login_failed = True
        login_error_msg = str(e)
    
    # Si el login falló, NO intentar ensure_dropi_token (solo causa más demora)
    if not token and login_failed:
        return {
            "connected": True,
            "error": "Token expirado, reconecta tu cuenta",
            "wallet": {"balance": 0, "currency": "COP"},
            "orders": {
                "total": 0, "pending": 0, "delivered": 0, "returned": 0,
                "delivery_rate": 0, "net_profit": 0
            },
            "daily": []
        }
    
    # 1. Obtener órdenes del período
    orders_data = await _fetch_orders_stats(
        token=token,
        country=connection.country,
        start_date=start_date,
        end_date=end_date,
        days=days
    )
    
    # 2. Obtener TODO el wallet history para cruzar
    wallet_history_result = await dropi_request(
        "GET",
        "/api/historywallet",
        token,
        connection.country,
        params={
            "orderBy": "id",
            "orderDirection": "desc",
            "result_number": 1000,
            "start": 0,
            "textToSearch": "",
            "type": "null",
            "id": "null",
            "identification_code": "null",
            "user_id": user_id,
            "from": "2024-01-01",
            "until": datetime.now().strftime("%Y-%m-%d"),
            "wallet_id": 0
        }
    )
    
    # 3. Crear mapa de order_id -> pago/cobro
    pagos_por_order = {}
    cobros_por_order = {}
    
    if wallet_history_result.get("success"):
        records = wallet_history_result.get("data", {}).get("objects", [])
        
        for record in records:
            description = record.get("description", "").upper()
            order_id = record.get("order_id")
            amount = abs(float(record.get("amount", 0)))
            
            if order_id:
                if "ENTRADA POR GANANCIA EN LA ORDEN COMO DROPSHIPPER" in description:
                    pagos_por_order[order_id] = amount
                elif "SALIDA POR COBRO DE FLETE INICIAL" in description:
                    cobros_por_order[order_id] = amount
    
    # 4. Obtener órdenes raw para hacer el cruce detallado
    orders_raw = await _fetch_orders_for_reconciliation(
        token=token,
        country=connection.country,
        start_date=start_date,
        end_date=end_date,
        days=days
    )
    
    # 5. Cruzar datos: órdenes del período vs pagos en wallet
    reconciliation = {
        "entregas_cobradas": 0,
        "entregas_cobradas_monto": 0,
        "entregas_pendientes": 0,
        "entregas_pendientes_monto": 0,
        "devoluciones_cobradas": 0,
        "devoluciones_cobradas_monto": 0,
        "devoluciones_pendientes": 0,
        "devoluciones_pendientes_monto": 0,
        "en_ruta": 0,
        "en_ruta_monto": 0,
    }
    
    # Daily data para gráfica con cruce real
    daily_reconciled = {}
    
    DELIVERED_STATES = {"ENTREGADO"}
    RETURNED_STATES = {"DEVOLUCION", "DEVOLUCIÓN"}
    CANCELLED_STATES = {"CANCELADO"}
    PENDING_CONFIRMATION_STATES = {"PENDIENTE", "PENDIENTE CONFIRMACION", "PENDIENTE CONFIRMACIÓN"}
    
    for order in orders_raw:
        order_id = order.get("id")
        status_upper = str(order.get("status", "")).upper().strip()
        profit = float(order.get("dropshipper_amount_to_win", 0))
        created_str = order.get("created_at", "")[:10]
        
        # Inicializar día si no existe
        if created_str not in daily_reconciled:
            daily_reconciled[created_str] = {
                "date": created_str,
                "ganancias_cobradas": 0,
                "ganancias_pendientes": 0,
                "devoluciones_cobradas": 0,
                "devoluciones_pendientes": 0,
                "en_ruta": 0
            }
        
        if status_upper in DELIVERED_STATES:
            if order_id in pagos_por_order:
                reconciliation["entregas_cobradas"] += 1
                reconciliation["entregas_cobradas_monto"] += pagos_por_order[order_id]
                daily_reconciled[created_str]["ganancias_cobradas"] += pagos_por_order[order_id]
            else:
                reconciliation["entregas_pendientes"] += 1
                reconciliation["entregas_pendientes_monto"] += profit
                daily_reconciled[created_str]["ganancias_pendientes"] += profit
                
        elif status_upper in RETURNED_STATES:
            if order_id in cobros_por_order:
                reconciliation["devoluciones_cobradas"] += 1
                reconciliation["devoluciones_cobradas_monto"] += cobros_por_order[order_id]
                daily_reconciled[created_str]["devoluciones_cobradas"] += cobros_por_order[order_id]
            else:
                costo_estimado = 23000
                reconciliation["devoluciones_pendientes"] += 1
                reconciliation["devoluciones_pendientes_monto"] += costo_estimado
                daily_reconciled[created_str]["devoluciones_pendientes"] += costo_estimado
                
        elif status_upper not in CANCELLED_STATES and status_upper not in PENDING_CONFIRMATION_STATES:
            reconciliation["en_ruta"] += 1
            reconciliation["en_ruta_monto"] += profit
            daily_reconciled[created_str]["en_ruta"] += profit
    
    # Calcular totales
    reconciliation["total_ganancias"] = reconciliation["entregas_cobradas_monto"] + reconciliation["entregas_pendientes_monto"]
    reconciliation["total_devoluciones"] = reconciliation["devoluciones_cobradas_monto"] + reconciliation["devoluciones_pendientes_monto"]
    reconciliation["utilidad_neta"] = reconciliation["total_ganancias"] - reconciliation["total_devoluciones"]
    reconciliation["utilidad_cobrada"] = reconciliation["entregas_cobradas_monto"] - reconciliation["devoluciones_cobradas_monto"]
    reconciliation["pendiente_neto"] = reconciliation["entregas_pendientes_monto"] - reconciliation["devoluciones_pendientes_monto"]
    
    # Convertir daily a lista ordenada
    daily_reconciled_list = sorted(daily_reconciled.values(), key=lambda x: x["date"])
    
    # Agregar display_date para la gráfica
    for item in daily_reconciled_list:
        try:
            dt = datetime.strptime(item["date"], "%Y-%m-%d")
            item["display_date"] = dt.strftime("%d/%m")
        except:
            item["display_date"] = item["date"]
    
    return {
        "connected": True,
        "wallet": {
            "balance": wallet_balance,
            "currency": "COP" if connection.country == "co" else "GTQ"
        },
        "orders": orders_data["stats"],
        "period": orders_data["period"],
        "daily": orders_data.get("daily", []),
        "reconciliation": reconciliation,
        "daily_reconciled": daily_reconciled_list
    }


async def _fetch_orders_for_reconciliation(token: str, country: str, start_date: str = None, end_date: str = None, days: int = 7) -> list:
    """Obtener órdenes raw para reconciliación"""
    
    # Calcular rango de fechas
    if start_date and end_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    else:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days)
    
    result = await dropi_request(
        "GET",
        "/api/orders/myorders",
        token,
        country,
        params={
            "result_number": 500,
            "order_by": "created_at",
            "order_dir": "desc"
        }
    )
    
    if not result.get("success"):
        return []
    
    orders = result.get("data", {}).get("objects", [])
    
    # Filtrar por fecha
    filtered_orders = []
    for order in orders:
        created_str = order.get("created_at", "")
        if not created_str:
            continue
        try:
            created_dt = datetime.strptime(created_str[:19], "%Y-%m-%dT%H:%M:%S")
            if start_dt <= created_dt <= end_dt:
                filtered_orders.append(order)
        except:
            continue
    
    return filtered_orders
