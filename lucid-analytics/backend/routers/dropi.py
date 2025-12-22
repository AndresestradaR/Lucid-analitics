"""
Router de Dropi - LEE DE CACHE LOCAL (PostgreSQL)
Los datos se sincronizan en background, las consultas son INSTANTÁNEAS.
"""

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional
from datetime import datetime, timedelta
from pydantic import BaseModel
import httpx
import asyncio

from database import get_db, User, DropiConnection, DropiOrder, DropiWalletHistory
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
    last_orders_sync: Optional[datetime] = None
    last_wallet_sync: Optional[datetime] = None
    sync_status: Optional[str] = None
    
    class Config:
        from_attributes = True


# ========== HELPERS ==========

def get_dropi_headers(token: str = None, country: str = "co"):
    """Headers para requests a Dropi"""
    origin = f"https://app.dropi.{country}"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": origin,
        "Referer": f"{origin}/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
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
    """Login en Dropi con timeout real"""
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
        async with asyncio.timeout(15):
            async with httpx.AsyncClient(timeout=httpx.Timeout(10.0, connect=5.0)) as client:
                response = await client.post(
                    f"{api_url}/api/login",
                    json=payload,
                    headers=get_dropi_headers(country=country)
                )
                
                try:
                    data = response.json()
                except:
                    return {"success": False, "error": "Respuesta inválida de Dropi"}
                
                if data.get("isSuccess") and data.get("token"):
                    user_data = data.get("objects", {})
                    
                    # Extraer wallet
                    wallet_balance = 0
                    wallet_obj = user_data.get("wallet")
                    if isinstance(wallet_obj, dict):
                        wallet_balance = float(wallet_obj.get("amount", 0) or 0)
                    elif wallet_obj is not None:
                        try:
                            wallet_balance = float(wallet_obj)
                        except:
                            pass
                    
                    return {
                        "success": True,
                        "token": data["token"],
                        "user_id": str(user_data.get("id", "")),
                        "user_name": f"{user_data.get('name', '')} {user_data.get('surname', '')}".strip(),
                        "wallet_balance": wallet_balance
                    }
                else:
                    return {"success": False, "error": data.get("message", "Login failed")}
                    
    except asyncio.TimeoutError:
        return {"success": False, "error": "Dropi no responde (timeout 15s)"}
    except Exception as e:
        return {"success": False, "error": str(e)[:100]}


# ========== ENDPOINTS ==========

@router.post("/connect", response_model=DropiConnectionResponse)
async def connect_dropi(
    data: DropiConnectRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Conectar cuenta de Dropi y disparar sincronización inicial"""
    
    if data.country not in DROPI_API_URLS:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"País no soportado. Opciones: {', '.join(DROPI_API_URLS.keys())}"
        )
    
    result = await dropi_login(data.email, data.password, data.country)
    
    if not result.get("success"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error de autenticación: {result.get('error')}"
        )
    
    existing = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id
    ).first()
    
    if existing:
        existing.email_encrypted = encrypt_token(data.email)
        existing.password_encrypted = encrypt_token(data.password)
        existing.country = data.country
        existing.current_token = result["token"]
        existing.token_expires_at = datetime.utcnow() + timedelta(hours=24)
        existing.dropi_user_id = result.get("user_id")
        existing.dropi_user_name = result.get("user_name")
        existing.is_active = True
        existing.sync_status = "pending"
        existing.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(existing)
        connection = existing
    else:
        connection = DropiConnection(
            user_id=current_user.id,
            email_encrypted=encrypt_token(data.email),
            password_encrypted=encrypt_token(data.password),
            country=data.country,
            current_token=result["token"],
            token_expires_at=datetime.utcnow() + timedelta(hours=24),
            dropi_user_id=result.get("user_id"),
            dropi_user_name=result.get("user_name"),
            sync_status="pending"
        )
        db.add(connection)
        db.commit()
        db.refresh(connection)
    
    # Disparar sync en background
    from routers.sync_dropi import sync_dropi_background
    background_tasks.add_task(sync_dropi_background, current_user.id)
    
    return DropiConnectionResponse.model_validate(connection)


@router.get("/status")
async def get_dropi_status(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Estado de conexión y sincronización de Dropi"""
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id,
        DropiConnection.is_active == True
    ).first()
    
    if not connection:
        return {"connected": False}
    
    # Contar datos en caché
    total_orders = db.query(func.count(DropiOrder.id)).filter(
        DropiOrder.user_id == current_user.id
    ).scalar() or 0
    
    total_wallet = db.query(func.count(DropiWalletHistory.id)).filter(
        DropiWalletHistory.user_id == current_user.id
    ).scalar() or 0
    
    return {
        "connected": True,
        "country": connection.country,
        "dropi_user_id": connection.dropi_user_id,
        "dropi_user_name": connection.dropi_user_name,
        "sync_status": connection.sync_status,
        "last_orders_sync": connection.last_orders_sync.isoformat() if connection.last_orders_sync else None,
        "last_wallet_sync": connection.last_wallet_sync.isoformat() if connection.last_wallet_sync else None,
        "cached_orders": total_orders,
        "cached_wallet_movements": total_wallet,
        "created_at": connection.created_at.isoformat()
    }


@router.post("/sync")
async def trigger_sync(
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Disparar sincronización manual"""
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id,
        DropiConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="No hay conexión de Dropi")
    
    if connection.sync_status == "syncing":
        return {"message": "Sincronización ya en progreso", "status": "syncing"}
    
    from routers.sync_dropi import sync_dropi_background
    background_tasks.add_task(sync_dropi_background, current_user.id)
    
    connection.sync_status = "syncing"
    db.commit()
    
    return {"message": "Sincronización iniciada", "status": "syncing"}


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
    """Obtener saldo actual de wallet (requiere login para dato en tiempo real)"""
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id,
        DropiConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="No hay conexión de Dropi")
    
    # Login fresco para balance actual
    email = decrypt_token(connection.email_encrypted)
    password = decrypt_token(connection.password_encrypted)
    
    result = await dropi_login(email, password, connection.country)
    
    balance = 0
    if result.get("success"):
        balance = result.get("wallet_balance", 0)
        connection.current_token = result["token"]
        connection.token_expires_at = datetime.utcnow() + timedelta(hours=24)
        db.commit()
    
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
    """
    Historial de wallet desde CACHE LOCAL - INSTANTÁNEO!
    """
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id,
        DropiConnection.is_active == True
    ).first()
    
    if not connection:
        return {"movements": [], "summary": {"total_in": 0, "total_out": 0, "net": 0, "count": 0}, "daily": [], "period": {}}
    
    # Calcular fechas
    if start_date and end_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    else:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days)
    
    # ========== QUERY DESDE CACHE LOCAL ==========
    movements = db.query(DropiWalletHistory).filter(
        DropiWalletHistory.user_id == current_user.id,
        DropiWalletHistory.movement_created_at >= start_dt,
        DropiWalletHistory.movement_created_at <= end_dt
    ).order_by(DropiWalletHistory.movement_created_at.desc()).limit(500).all()
    
    # Calcular totales
    total_in = 0
    total_out = 0
    total_ganancias = 0
    total_devoluciones = 0
    count_ganancias = 0
    count_devoluciones = 0
    daily_data = {}
    daily_dropshipping = {}
    
    # Inicializar días
    current_day = start_dt
    while current_day <= end_dt:
        day_key = current_day.strftime("%Y-%m-%d")
        daily_data[day_key] = {"ingresos": 0, "egresos": 0, "date": day_key}
        daily_dropshipping[day_key] = {"ganancias": 0, "devoluciones": 0, "date": day_key}
        current_day += timedelta(days=1)
    
    formatted_movements = []
    for mov in movements:
        day_key = mov.movement_created_at.strftime("%Y-%m-%d")
        amount = float(mov.amount or 0)
        
        if mov.movement_type == "ENTRADA":
            total_in += amount
            if day_key in daily_data:
                daily_data[day_key]["ingresos"] += amount
            
            if mov.category == "ganancia_dropshipping":
                total_ganancias += amount
                count_ganancias += 1
                if day_key in daily_dropshipping:
                    daily_dropshipping[day_key]["ganancias"] += amount
        else:
            total_out += amount
            if day_key in daily_data:
                daily_data[day_key]["egresos"] += amount
            
            if mov.category == "cobro_flete":
                total_devoluciones += amount
                count_devoluciones += 1
                if day_key in daily_dropshipping:
                    daily_dropshipping[day_key]["devoluciones"] += amount
        
        formatted_movements.append({
            "id": mov.dropi_wallet_id,
            "amount": amount,
            "balance": float(mov.balance_after or 0),
            "description": mov.description,
            "type": mov.movement_type,
            "category": mov.category,
            "order_id": mov.order_id,
            "created_at": mov.movement_created_at.isoformat()
        })
    
    # Formatear daily
    daily_list = sorted(daily_data.values(), key=lambda x: x["date"])
    daily_drop_list = sorted(daily_dropshipping.values(), key=lambda x: x["date"])
    
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
    
    promedio_ganancia = round(total_ganancias / count_ganancias, 2) if count_ganancias > 0 else 0
    promedio_devolucion = round(total_devoluciones / count_devoluciones, 2) if count_devoluciones > 0 else 0
    
    return {
        "movements": formatted_movements[:100],
        "summary": {
            "total_in": total_in,
            "total_out": total_out,
            "net": total_in - total_out,
            "count": len(movements)
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


@router.get("/summary")
async def get_dropi_summary(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days: int = 7,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Resumen completo de Dropi desde CACHE LOCAL - INSTANTÁNEO!
    """
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id,
        DropiConnection.is_active == True
    ).first()
    
    if not connection:
        return {
            "connected": False,
            "message": "Conecta tu cuenta de Dropi para ver métricas"
        }
    
    # Calcular fechas
    if start_date and end_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    else:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days)
    
    # ========== QUERY ÓRDENES DESDE CACHE ==========
    orders = db.query(DropiOrder).filter(
        DropiOrder.user_id == current_user.id,
        DropiOrder.order_created_at >= start_dt,
        DropiOrder.order_created_at <= end_dt
    ).all()
    
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
    
    # Para reconciliación
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
    
    daily_data = {}
    daily_reconciled = {}
    
    for order in orders:
        status = order.status
        profit = float(order.dropshipper_profit or 0)
        total = float(order.total_order or 0)
        day_key = order.order_created_at.strftime("%Y-%m-%d")
        
        # Inicializar día
        if day_key not in daily_data:
            daily_data[day_key] = {
                "date": day_key,
                "delivered": 0,
                "returned": 0,
                "en_ruta": 0,
                "total": 0
            }
            daily_reconciled[day_key] = {
                "date": day_key,
                "ganancias_cobradas": 0,
                "ganancias_pendientes": 0,
                "devoluciones_cobradas": 0,
                "devoluciones_pendientes": 0,
                "en_ruta": 0
            }
        
        stats["total"] += 1
        stats["total_sales"] += total
        daily_data[day_key]["total"] += 1
        
        if status == "ENTREGADO":
            stats["delivered"] += 1
            stats["delivered_profit"] += profit
            daily_data[day_key]["delivered"] += 1
            
            # Reconciliación: ¿Ya pagado?
            if order.is_paid:
                reconciliation["entregas_cobradas"] += 1
                reconciliation["entregas_cobradas_monto"] += float(order.paid_amount or profit)
                daily_reconciled[day_key]["ganancias_cobradas"] += float(order.paid_amount or profit)
            else:
                reconciliation["entregas_pendientes"] += 1
                reconciliation["entregas_pendientes_monto"] += profit
                daily_reconciled[day_key]["ganancias_pendientes"] += profit
                
        elif status == "DEVOLUCION":
            stats["returned"] += 1
            daily_data[day_key]["returned"] += 1
            
            # Reconciliación: ¿Ya cobrado?
            if order.is_return_charged:
                costo = float(order.return_charged_amount or 23000)
                reconciliation["devoluciones_cobradas"] += 1
                reconciliation["devoluciones_cobradas_monto"] += costo
                daily_reconciled[day_key]["devoluciones_cobradas"] += costo
            else:
                costo_estimado = 23000
                reconciliation["devoluciones_pendientes"] += 1
                reconciliation["devoluciones_pendientes_monto"] += costo_estimado
                daily_reconciled[day_key]["devoluciones_pendientes"] += costo_estimado
            
            stats["return_cost"] += 23000
            
        elif status == "CANCELADO":
            stats["cancelled"] += 1
            
        elif status == "PENDIENTE_CONFIRMACION":
            stats["pending_confirmation"] += 1
            
        else:  # EN_RUTA y otros
            stats["en_ruta"] += 1
            stats["pending_profit"] += profit
            daily_data[day_key]["en_ruta"] += 1
            
            reconciliation["en_ruta"] += 1
            reconciliation["en_ruta_monto"] += profit
            daily_reconciled[day_key]["en_ruta"] += profit
    
    # Calcular métricas
    stats["net_profit"] = stats["delivered_profit"] - stats["return_cost"]
    
    completed = stats["delivered"] + stats["returned"]
    stats["effective_delivery_rate"] = round((stats["delivered"] / completed * 100) if completed > 0 else 0, 1)
    stats["effective_return_rate"] = round((stats["returned"] / completed * 100) if completed > 0 else 0, 1)
    stats["total_operativo"] = stats["delivered"] + stats["returned"] + stats["en_ruta"]
    stats["cancellation_rate"] = round((stats["cancelled"] / stats["total"] * 100) if stats["total"] > 0 else 0, 1)
    stats["completion_rate"] = round((completed / stats["total_operativo"] * 100) if stats["total_operativo"] > 0 else 0, 1)
    stats["delivery_rate"] = round((stats["delivered"] / stats["total"] * 100) if stats["total"] > 0 else 0, 1)
    
    # Calcular totales de reconciliación
    reconciliation["total_ganancias"] = reconciliation["entregas_cobradas_monto"] + reconciliation["entregas_pendientes_monto"]
    reconciliation["total_devoluciones"] = reconciliation["devoluciones_cobradas_monto"] + reconciliation["devoluciones_pendientes_monto"]
    reconciliation["utilidad_neta"] = reconciliation["total_ganancias"] - reconciliation["total_devoluciones"]
    reconciliation["utilidad_cobrada"] = reconciliation["entregas_cobradas_monto"] - reconciliation["devoluciones_cobradas_monto"]
    reconciliation["pendiente_neto"] = reconciliation["entregas_pendientes_monto"] - reconciliation["devoluciones_pendientes_monto"]
    
    # Formatear daily
    daily_list = sorted(daily_data.values(), key=lambda x: x["date"])
    daily_reconciled_list = sorted(daily_reconciled.values(), key=lambda x: x["date"])
    
    for item in daily_reconciled_list:
        try:
            dt = datetime.strptime(item["date"], "%Y-%m-%d")
            item["display_date"] = dt.strftime("%d/%m")
        except:
            item["display_date"] = item["date"]
    
    # Obtener wallet balance actual (esto SÍ requiere login)
    wallet_balance = 0
    try:
        email = decrypt_token(connection.email_encrypted)
        password = decrypt_token(connection.password_encrypted)
        login_result = await dropi_login(email, password, connection.country)
        if login_result.get("success"):
            wallet_balance = login_result.get("wallet_balance", 0)
    except:
        pass
    
    return {
        "connected": True,
        "wallet": {
            "balance": wallet_balance,
            "currency": "COP" if connection.country == "co" else "GTQ"
        },
        "orders": stats,
        "period": {
            "start": start_dt.strftime("%Y-%m-%d"),
            "end": end_dt.strftime("%Y-%m-%d")
        },
        "daily": daily_list,
        "reconciliation": reconciliation,
        "daily_reconciled": daily_reconciled_list,
        "sync_status": connection.sync_status,
        "last_sync": connection.last_orders_sync.isoformat() if connection.last_orders_sync else None
    }


@router.get("/orders")
async def get_orders(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    days: int = 7,
    status_filter: Optional[str] = None,
    limit: int = 100,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener órdenes desde cache local - INSTANTÁNEO"""
    connection = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id,
        DropiConnection.is_active == True
    ).first()
    
    if not connection:
        raise HTTPException(status_code=404, detail="No hay conexión de Dropi")
    
    # Calcular fechas
    if start_date and end_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
    else:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days)
    
    # Query desde cache
    query = db.query(DropiOrder).filter(
        DropiOrder.user_id == current_user.id,
        DropiOrder.order_created_at >= start_dt,
        DropiOrder.order_created_at <= end_dt
    )
    
    if status_filter:
        query = query.filter(DropiOrder.status == status_filter.upper())
    
    orders = query.order_by(DropiOrder.order_created_at.desc()).limit(limit).all()
    
    formatted_orders = []
    for order in orders:
        formatted_orders.append({
            "id": order.dropi_order_id,
            "status": order.status,
            "status_raw": order.status_raw,
            "customer": order.customer_name,
            "phone": order.customer_phone,
            "city": order.customer_city,
            "total": float(order.total_order or 0),
            "profit": float(order.dropshipper_profit or 0),
            "shipping_guide": order.shipping_guide,
            "is_paid": order.is_paid,
            "is_return_charged": order.is_return_charged,
            "created_at": order.order_created_at.isoformat()
        })
    
    return {
        "orders": formatted_orders,
        "count": len(formatted_orders),
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
    """Detalle de una orden desde cache"""
    order = db.query(DropiOrder).filter(
        DropiOrder.user_id == current_user.id,
        DropiOrder.dropi_order_id == order_id
    ).first()
    
    if not order:
        raise HTTPException(status_code=404, detail="Orden no encontrada")
    
    import json
    products = []
    if order.products_json:
        try:
            products = json.loads(order.products_json)
        except:
            pass
    
    return {
        "id": order.dropi_order_id,
        "status": order.status,
        "status_raw": order.status_raw,
        "customer": {
            "name": order.customer_name,
            "phone": order.customer_phone,
            "address": order.customer_address,
            "city": order.customer_city,
            "state": order.customer_state
        },
        "financials": {
            "total": float(order.total_order or 0),
            "shipping": float(order.shipping_amount or 0),
            "profit": float(order.dropshipper_profit or 0)
        },
        "shipping": {
            "guide": order.shipping_guide,
            "company": order.shipping_company,
            "type": order.rate_type
        },
        "products": products,
        "payment": {
            "is_paid": order.is_paid,
            "paid_at": order.paid_at.isoformat() if order.paid_at else None,
            "paid_amount": float(order.paid_amount or 0) if order.paid_amount else None
        },
        "return": {
            "is_charged": order.is_return_charged,
            "charged_at": order.return_charged_at.isoformat() if order.return_charged_at else None,
            "charged_amount": float(order.return_charged_amount or 0) if order.return_charged_amount else None
        },
        "created_at": order.order_created_at.isoformat(),
        "updated_at": order.order_updated_at.isoformat() if order.order_updated_at else None
    }


@router.post("/test-login")
async def test_dropi_login(data: DropiConnectRequest):
    """Test de login sin guardar nada - para debug"""
    result = await dropi_login(data.email, data.password, data.country)
    
    if result.get("success"):
        return {
            "success": True,
            "user_id": result.get("user_id"),
            "user_name": result.get("user_name"),
            "wallet_balance": result.get("wallet_balance"),
            "message": "Login exitoso"
        }
    else:
        return {
            "success": False,
            "error": result.get("error"),
            "message": f"Login falló: {result.get('error')}"
        }
