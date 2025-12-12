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

# white_brand_id por país
WHITE_BRAND_IDS = {
    "gt": 1,
    "co": "df3e6b0bb66ceaadca4f84cbc371fd66e04d20fe51fc414da8d1b84d31d178de",
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

def get_dropi_headers(token: str = None):
    """Headers para requests a Dropi"""
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://app.dropi.co",
        "Referer": "https://app.dropi.co/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


async def dropi_login(email: str, password: str, country: str) -> dict:
    """Hace login en Dropi y obtiene el token"""
    api_url = DROPI_API_URLS.get(country, DROPI_API_URLS["co"])
    white_brand_id = WHITE_BRAND_IDS.get(country, WHITE_BRAND_IDS["co"])
    
    url = f"{api_url}/api/login"
    payload = {
        "email": email,
        "password": password,
        "white_brand_id": white_brand_id,
        "brand": "",
        "otp": None,
        "with_cdc": False
    }
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(url, json=payload, headers=get_dropi_headers())
            data = response.json()
            
            if data.get("isSuccess") and data.get("token"):
                user_data = data.get("objects", {})
                return {
                    "success": True, 
                    "token": data["token"],
                    "user_id": str(user_data.get("id", "")),
                    "user_name": f"{user_data.get('name', '')} {user_data.get('surname', '')}".strip()
                }
            else:
                return {"success": False, "error": data.get("message", "Login fallido")}
        except Exception as e:
            return {"success": False, "error": str(e)}


async def dropi_request(method: str, endpoint: str, token: str, country: str, params: dict = None, payload: dict = None) -> dict:
    """Request genérico a la API de Dropi"""
    api_url = DROPI_API_URLS.get(country, DROPI_API_URLS["co"])
    url = f"{api_url}{endpoint}"
    
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            if method == "GET":
                response = await client.get(url, headers=get_dropi_headers(token), params=params)
            else:
                response = await client.post(url, headers=get_dropi_headers(token), json=payload)
            
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
    """Historial de movimientos de la wallet"""
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
        params={"result_number": 500}
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
    
    for record in records:
        created_str = record.get("created_at", "")
        if created_str:
            try:
                created_dt = datetime.strptime(created_str[:19], "%Y-%m-%dT%H:%M:%S")
                if start_dt <= created_dt <= end_dt:
                    amount = float(record.get("amount", 0))
                    if amount > 0:
                        total_in += amount
                    else:
                        total_out += abs(amount)
                    
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
    
    return {
        "movements": filtered[:100],  # Limitar a 100
        "summary": {
            "total_in": total_in,
            "total_out": total_out,
            "net": total_in - total_out,
            "count": len(filtered)
        },
        "period": {
            "start": start_dt.strftime("%Y-%m-%d"),
            "end": end_dt.strftime("%Y-%m-%d")
        }
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
    
    token = await ensure_dropi_token(connection, db)
    
    # Obtener wallet
    wallet_result = await dropi_request(
        "GET",
        "/api/historywallet",
        token,
        connection.country,
        params={"result_number": 1}
    )
    
    wallet_balance = 0
    if wallet_result.get("success"):
        records = wallet_result.get("data", {}).get("objects", [])
        if records:
            wallet_balance = float(records[0].get("balance", 0))
    
    # Obtener órdenes
    orders_data = await get_orders(
        start_date=start_date,
        end_date=end_date,
        days=days,
        current_user=current_user,
        db=db
    )
    
    return {
        "connected": True,
        "wallet": {
            "balance": wallet_balance,
            "currency": "COP" if connection.country == "co" else "GTQ"
        },
        "orders": orders_data["stats"],
        "period": orders_data["period"]
    }
