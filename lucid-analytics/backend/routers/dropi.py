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
    IDÉNTICO al MCP server que funciona
    """
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
    
    print(f"[DROPI DEBUG] URL: {url}")
    print(f"[DROPI DEBUG] Country: {country}")
    print(f"[DROPI DEBUG] Email: {email}")
    print(f"[DROPI DEBUG] white_brand_id type: {type(white_brand_id)}")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(
                url, 
                json=payload, 
                headers=get_dropi_headers(country=country)
            )
            
            print(f"[DROPI DEBUG] Status: {response.status_code}")
            print(f"[DROPI DEBUG] Response: {response.text[:500]}")
            
            # Intentar parsear respuesta
            try:
                data = response.json()
            except:
                return {"success": False, "error": f"Respuesta inválida de Dropi: {response.text[:200]}"}
            
            # Verificar respuesta exitosa
            if data.get("isSuccess") and data.get("token"):
                user_data = data.get("objects", {})
                
                # Extraer wallet del usuario - múltiples formatos posibles
                wallet_balance = 0
                
                # Formato 1: wallet como objeto con amount
                wallet_obj = user_data.get("wallet")
                if isinstance(wallet_obj, dict):
                    wallet_balance = float(wallet_obj.get("amount", 0) or 0)
                    print(f"[DROPI DEBUG] Wallet from wallet.amount: {wallet_balance}")
                elif wallet_obj is not None:
                    # Formato 2: wallet como número directo
                    try:
                        wallet_balance = float(wallet_obj)
                        print(f"[DROPI DEBUG] Wallet from wallet (direct): {wallet_balance}")
                    except:
                        pass
                
                # Formato 3: wallets como array
                if wallet_balance == 0:
                    wallets = user_data.get("wallets", [])
                    if wallets and isinstance(wallets, list):
                        for w in wallets:
                            if isinstance(w, dict) and w.get("amount"):
                                wallet_balance = float(w.get("amount", 0))
                                print(f"[DROPI DEBUG] Wallet from wallets[]: {wallet_balance}")
                                break
                
                # Formato 4: balance directo en user
                if wallet_balance == 0:
                    balance = user_data.get("balance")
                    if balance:
                        try:
                            wallet_balance = float(balance)
                            print(f"[DROPI DEBUG] Wallet from balance: {wallet_balance}")
                        except:
                            pass
                
                # DEBUG: mostrar estructura de wallet para diagnóstico
                print(f"[DROPI DEBUG] user_data keys: {list(user_data.keys())[:10]}")
                print(f"[DROPI DEBUG] wallet raw: {user_data.get('wallet')}")
                print(f"[DROPI DEBUG] wallets raw: {user_data.get('wallets')}")
                print(f"[DROPI DEBUG] Final wallet_balance: {wallet_balance}")
                
                return {
                    "success": True, 
                    "token": data["token"],
                    "user_id": str(user_data.get("id", "")),
                    "user_name": f"{user_data.get('name', '')} {user_data.get('surname', '')}".strip(),
                    "wallet_balance": wallet_balance
                }
            else:
                # Extraer mensaje de error
                error_msg = data.get("message", "")
                if not error_msg:
                    error_msg = data.get("error", "")
                if not error_msg:
                    error_msg = str(data)[:200]
                return {"success": False, "error": error_msg or "Login fallido"}
                
        except httpx.TimeoutException:
            return {"success": False, "error": "Timeout conectando con Dropi"}
        except httpx.RequestError as e:
            return {"success": False, "error": f"Error de conexión: {str(e)}"}


async def dropi_request(method: str, endpoint: str, token: str, country: str, params: dict = None, payload: dict = None) -> dict:
    """Request genérico a la API de Dropi"""
    api_url = DROPI_API_URLS.get(country, DROPI_API_URLS["co"])
    url = f"{api_url}{endpoint}"
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            if method == "GET":
                response = await client.get(url, headers=get_dropi_headers(token, country), params=params)
            else:
                response = await client.post(url, headers=get_dropi_headers(token, country), json=payload)
            
            if response.status_code == 401:
                return {"success": False, "error": "Token expirado", "expired": True}
            
            if response.status_code == 200:
                data = response.json()
                return {"success": True, "data": data}
            else:
                return {"success": False, "error": f"HTTP {response.status_code}"}
        except Exception as e:
            return {"success": False, "error": str(e)}


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
        raise HTTPException(status_code=404, detail="No hay conexión de Dropi")
    
    token = await ensure_dropi_token(connection, db)
    
    result = await dropi_request(
        "GET",
        "/api/historywallet",
        token,
        connection.country,
        params={"result_number": 1000}
    )
    
    if not result.get("success"):
        raise HTTPException(status_code=400, detail=result.get("error"))
    
    data = result.get("data", {})
    records = data.get("objects", [])
    
    # Filtrar por fechas si se especifican
    if start_date and end_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    else:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days)
    
    filtered = []
    total_in = 0
    total_out = 0
    
    # Diccionario para agrupar por día
    daily_data = {}
    
    # Inicializar todos los días del período con 0
    current_day = start_dt
    while current_day <= end_dt:
        day_key = current_day.strftime("%Y-%m-%d")
        daily_data[day_key] = {"ingresos": 0, "egresos": 0, "date": day_key}
        current_day += timedelta(days=1)
    
    for record in records:
        created_str = record.get("created_at", "")
        if created_str:
            try:
                created_dt = datetime.strptime(created_str[:19], "%Y-%m-%dT%H:%M:%S")
                if start_dt <= created_dt <= end_dt:
                    amount = float(record.get("amount", 0))
                    day_key = created_dt.strftime("%Y-%m-%d")
                    
                    if amount > 0:
                        total_in += amount
                        if day_key in daily_data:
                            daily_data[day_key]["ingresos"] += amount
                    else:
                        total_out += abs(amount)
                        if day_key in daily_data:
                            daily_data[day_key]["egresos"] += abs(amount)
                    
                    filtered.append({
                        "id": record.get("id"),
                        "amount": amount,
                        "balance": float(record.get("balance", 0)),
                        "description": record.get("description", ""),
                        "type": record.get("type", ""),
                        "created_at": created_str
                    })
            except:
                pass
    
    # Convertir daily_data a lista ordenada por fecha
    daily_list = sorted(daily_data.values(), key=lambda x: x["date"])
    
    # Formatear fechas para mostrar en gráfica (DD/MM)
    for item in daily_list:
        date_obj = datetime.strptime(item["date"], "%Y-%m-%d")
        item["display_date"] = date_obj.strftime("%d/%m")
    
    return {
        "movements": filtered[:100],  # Limitar a 100
        "summary": {
            "total_in": total_in,
            "total_out": total_out,
            "net": total_in - total_out,
            "count": len(filtered)
        },
        "daily": daily_list,
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
    
    print(f"[DROPI DEBUG] Fetching orders from {start_dt} to {end_dt}")
    
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
        print(f"[DROPI DEBUG] Error fetching orders: {result.get('error')}")
        return {
            "stats": {
                "total": 0, "pending": 0, "confirmed": 0, "shipped": 0,
                "delivered": 0, "returned": 0, "cancelled": 0,
                "total_sales": 0, "delivered_profit": 0, "pending_profit": 0,
                "return_cost": 0, "net_profit": 0, "delivery_rate": 0
            },
            "period": {"start": start_dt.strftime("%Y-%m-%d"), "end": end_dt.strftime("%Y-%m-%d")},
            "error": result.get("error")
        }
    
    data = result.get("data", {})
    orders = data.get("objects", [])
    
    print(f"[DROPI DEBUG] Total orders from API: {len(orders)}")
    
    # DEBUG: Ver los primeros 3 órdenes para entender la estructura
    if orders:
        for i, order in enumerate(orders[:3]):
            print(f"[DROPI DEBUG] Order {i}: status={order.get('status')}, status_id={order.get('status_id')}, state={order.get('state')}, created_at={order.get('created_at', '')[:10]}")
    
    # Mapeo de estados - Dropi Colombia usa STRINGS en español
    STATUS_MAP = {
        # Strings en español (Colombia)
        "ENTREGADO": "delivered",
        "DEVOLUCION": "returned",
        "DEVOLUCIÓN": "returned",
        "CANCELADO": "cancelled",
        "PENDIENTE": "pending",
        "PENDIENTE CONFIRMACION": "pending",
        "PENDIENTE CONFIRMACIÓN": "pending",
        "CONFIRMADO": "confirmed",
        "ENVIADO": "shipped",
        "EN CAMINO": "shipped",
        "NOVEDAD": "shipped",  # Novedad = en tránsito con algún issue
        "EN BODEGA": "confirmed",
        # También soportar números por si acaso
        1: "pending", 2: "confirmed", 3: "shipped",
        4: "delivered", 5: "returned", 6: "cancelled",
        "1": "pending", "2": "confirmed", "3": "shipped",
        "4": "delivered", "5": "returned", "6": "cancelled",
    }
    
    # Contadores
    stats = {
        "total": 0, "pending": 0, "confirmed": 0, "shipped": 0,
        "delivered": 0, "returned": 0, "cancelled": 0,
        "total_sales": 0, "delivered_profit": 0, "pending_profit": 0, "return_cost": 0,
    }
    
    status_debug = {}  # Para contar qué status recibimos
    daily_data = {}  # Para gráfico por día
    
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
        
        # Obtener status - es un string directo en español
        status_raw = order.get("status", "")
        if isinstance(status_raw, dict):
            status_raw = status_raw.get("name", status_raw.get("id", "unknown"))
        
        # Convertir a mayúsculas para comparar
        status_upper = str(status_raw).upper().strip()
        status_name = STATUS_MAP.get(status_upper, STATUS_MAP.get(status_raw, "unknown"))
        
        # Debug: contar status
        status_debug[status_upper] = status_debug.get(status_upper, 0) + 1
        
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
        
        # Agregar a datos diarios para gráfico
        day_key = created_dt.strftime("%Y-%m-%d")
        if day_key not in daily_data:
            daily_data[day_key] = {"date": day_key, "delivered": 0, "returned": 0, "pending": 0, "total": 0}
        daily_data[day_key]["total"] += 1
        if status_name == "delivered":
            daily_data[day_key]["delivered"] += 1
        elif status_name == "returned":
            daily_data[day_key]["returned"] += 1
        elif status_name in ["pending", "confirmed", "shipped"]:
            daily_data[day_key]["pending"] += 1
    
    # Convertir daily_data a lista ordenada por fecha
    daily_list = sorted(daily_data.values(), key=lambda x: x["date"])
    
    print(f"[DROPI DEBUG] Status distribution: {status_debug}")
    stats["net_profit"] = stats["delivered_profit"] - stats["return_cost"]
    stats["delivery_rate"] = round((stats["delivered"] / stats["total"] * 100) if stats["total"] > 0 else 0, 1)
    
    print(f"[DROPI DEBUG] Orders in range: {stats['total']}, Delivered: {stats['delivered']}, Daily points: {len(daily_list)}")
    
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
        1: "pending",      # Pendiente
        2: "confirmed",    # Confirmado
        3: "shipped",      # Enviado
        4: "delivered",    # Entregado
        5: "returned",     # Devuelto
        6: "cancelled",    # Cancelado
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
            stats["return_cost"] += 23000  # Costo fijo devolución Colombia
        
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
    """Resumen completo de Dropi para el Dashboard"""
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id,
        DropiConnection.is_active == True
    ).first()
    
    if not connection:
        return {
            "connected": False,
            "message": "Conecta tu cuenta de Dropi para ver métricas"
        }
    
    print(f"[DROPI DEBUG] Getting summary for user {current_user.id}, dates: {start_date} to {end_date}")
    
    token = await ensure_dropi_token(connection, db)
    
    if not token:
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
    
    # Obtener wallet haciendo login fresco (el wallet viene en la respuesta del login)
    wallet_balance = 0
    
    try:
        # Usar las funciones de utils que ya existen
        email = decrypt_token(connection.email_encrypted)
        password = decrypt_token(connection.password_encrypted)
        
        # Hacer login fresco para obtener wallet actualizado
        login_result = await dropi_login(email, password, connection.country)
        if login_result.get("success"):
            wallet_balance = login_result.get("wallet_balance", 0)
            print(f"[DROPI DEBUG] Wallet from fresh login: {wallet_balance}")
    except Exception as e:
        print(f"[DROPI DEBUG] Error getting wallet from login: {e}")
    
    print(f"[DROPI DEBUG] Final wallet balance: {wallet_balance}")
    
    # Obtener órdenes usando la función auxiliar
    orders_data = await _fetch_orders_stats(
        token=token,
        country=connection.country,
        start_date=start_date,
        end_date=end_date,
        days=days
    )
    
    return {
        "connected": True,
        "wallet": {
            "balance": wallet_balance,
            "currency": "COP" if connection.country == "co" else "GTQ"
        },
        "orders": orders_data["stats"],
        "period": orders_data["period"],
        "daily": orders_data.get("daily", [])
    }
