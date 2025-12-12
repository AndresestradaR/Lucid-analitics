"""
Router de Chat - El Cerebro
Asistente IA para análisis de rentabilidad
"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import datetime, timedelta
from pydantic import BaseModel
import httpx
import os
import json

from database import get_db, User, ChatHistory, MetaAccount, DropiConnection, LucidbotConnection
from routers.auth import get_current_user
from utils import decrypt_token

router = APIRouter()


# ========== SCHEMAS ==========

class ChatMessage(BaseModel):
    message: str
    context: Optional[dict] = None  # Contexto opcional del dashboard


class ChatResponse(BaseModel):
    response: str
    data_used: Optional[dict] = None


# ========== SYSTEM PROMPT ==========

SYSTEM_PROMPT = """Eres "El Cerebro", el asistente financiero de un negocio de Dropshipping COD (Cash on Delivery).
Hoy es {today}.

## TU MISIÓN
Ayudar al dueño a entender si está GANANDO o PERDIENDO dinero con análisis claros y directos.

## DATOS DISPONIBLES
Te pasaré datos de 3 fuentes:

### META ADS (Publicidad)
- Gasto total en campañas
- CPA, CTR, CPM
- Campañas activas

### DROPI (Logística y Fulfillment)
Estados de pedidos:
- **Entregados**: Pedidos completados exitosamente ✅
- **Devueltos**: Pedidos que no se pudieron entregar ↩️
- **En Ruta**: Pedidos en tránsito (novedad, en camino, en reparto, etc.) 🚚
- **Cancelados**: Pedidos cancelados antes de envío 🚫
- **Pendientes Confirmación**: Aún no se han enviado ⏳

Métricas financieras:
- Wallet: Saldo disponible en Dropi
- Ganancias: Dinero recibido por entregas
- Devoluciones: Dinero cobrado por fletes de devolución (~$15,000-25,000 COP c/u)
- Utilidad Neta: Ganancias - Devoluciones

### WALLET HISTORY (Movimientos reales de dinero)
- count_ganancias: Número de pagos recibidos por entregas
- count_devoluciones: Número de cobros por devoluciones
- promedio_ganancia: Ganancia promedio por entrega
- promedio_devolucion: Costo promedio por devolución

### CONCILIACIÓN
Comparación entre órdenes y movimientos de wallet:
- Entregas pendientes de pago: Órdenes entregadas que Dropi aún no te ha pagado
- Devoluciones pendientes de cobro: Devoluciones que Dropi aún no te ha descontado

## MÉTRICAS CLAVE

### Tasas Importantes
- **Tasa de Entrega Efectiva** = Entregados ÷ (Entregados + Devueltos) × 100
  - > 70% es bueno, > 80% excelente
- **Tasa de Devolución** = Devueltos ÷ (Entregados + Devueltos) × 100
- **Tasa de Cancelación** = Cancelados ÷ Total pedidos × 100
- **Operación Completada** = (Entregados + Devueltos) ÷ Total operativo × 100

### CPA (Costo Por Adquisición)
- CPA Inicial = Gasto Ads ÷ Pedidos totales
- **CPA Real** = Gasto Ads ÷ Pedidos ENTREGADOS (el que importa)

### Profit
- Profit Bruto = Ganancias de entregas (de wallet)
- **Profit Neto** = Utilidad Wallet - Gasto Ads
- ✅ Positivo = Ganando dinero
- ❌ Negativo = Perdiendo dinero

### ROAS
- ROAS = Revenue ÷ Gasto Ads
- > 2 es bueno, > 3 excelente

## ESTILO DE RESPUESTA
- Responde en ESPAÑOL
- Sé directo y conciso
- Usa emojis con moderación (máximo 3-4 por respuesta)
- Siempre da un veredicto claro: ¿Está ganando o perdiendo?
- Si faltan datos, indícalo claramente
- Formatea números grandes con separadores (ej: $2,668,576)
- Para montos en COP, usa el símbolo $

## DATOS DEL USUARIO
{user_data}

Analiza los datos y responde la pregunta del usuario de forma clara y útil.
"""


# ========== HELPERS ==========

async def get_meta_spend(token: str, account_id: str, start_date: str, end_date: str) -> dict:
    """Obtener gasto de Meta Ads"""
    url = f"https://graph.facebook.com/v21.0/act_{account_id}/insights"
    params = {
        "access_token": token,
        "level": "account",
        "fields": "spend,impressions,clicks,ctr,cpm,actions",
        "time_range": f'{{"since":"{start_date}","until":"{end_date}"}}'
    }
    
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            response = await client.get(url, params=params)
            if response.status_code == 200:
                data = response.json().get("data", [])
                if data:
                    return {
                        "spend": float(data[0].get("spend", 0)),
                        "impressions": int(data[0].get("impressions", 0)),
                        "clicks": int(data[0].get("clicks", 0)),
                        "ctr": float(data[0].get("ctr", 0)),
                        "cpm": float(data[0].get("cpm", 0))
                    }
        except:
            pass
    return {"spend": 0, "impressions": 0, "clicks": 0, "ctr": 0, "cpm": 0}


async def get_dropi_data(token: str, country: str, start_date: str, end_date: str) -> dict:
    """Obtener datos de Dropi con la nueva categorización de estados"""
    from routers.dropi import dropi_request
    
    # Obtener wallet
    wallet_result = await dropi_request(
        "GET", "/api/historywallet", token, country, params={"result_number": 1}
    )
    wallet_balance = 0
    if wallet_result.get("success"):
        records = wallet_result.get("data", {}).get("objects", [])
        if records:
            wallet_balance = float(records[0].get("balance", 0))
    
    # Obtener historial de wallet para ganancias/devoluciones
    wallet_history_result = await dropi_request(
        "GET", "/api/historywallet", token, country, params={"result_number": 500}
    )
    
    wallet_stats = {
        "total_ganancias": 0,
        "total_devoluciones": 0,
        "count_ganancias": 0,
        "count_devoluciones": 0
    }
    
    if wallet_history_result.get("success"):
        records = wallet_history_result.get("data", {}).get("objects", [])
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        
        for record in records:
            created_str = record.get("created_at", "")
            if not created_str:
                continue
            try:
                created_dt = datetime.strptime(created_str[:19], "%Y-%m-%dT%H:%M:%S")
                if not (start_dt <= created_dt <= end_dt):
                    continue
            except:
                continue
            
            description = record.get("description", "").upper()
            amount = abs(float(record.get("amount", 0)))
            
            if "ENTRADA POR GANANCIA EN LA ORDEN COMO DROPSHIPPER" in description:
                wallet_stats["total_ganancias"] += amount
                wallet_stats["count_ganancias"] += 1
            elif "SALIDA POR COBRO DE FLETE INICIAL" in description:
                wallet_stats["total_devoluciones"] += amount
                wallet_stats["count_devoluciones"] += 1
    
    # Calcular promedios
    wallet_stats["promedio_ganancia"] = round(
        wallet_stats["total_ganancias"] / wallet_stats["count_ganancias"], 2
    ) if wallet_stats["count_ganancias"] > 0 else 0
    
    wallet_stats["promedio_devolucion"] = round(
        wallet_stats["total_devoluciones"] / wallet_stats["count_devoluciones"], 2
    ) if wallet_stats["count_devoluciones"] > 0 else 0
    
    wallet_stats["utilidad_neta"] = wallet_stats["total_ganancias"] - wallet_stats["total_devoluciones"]
    
    # Obtener órdenes
    orders_result = await dropi_request(
        "GET", "/api/orders/myorders", token, country,
        params={"result_number": 500, "order_by": "created_at", "order_dir": "desc"}
    )
    
    # Nueva categorización de estados
    DELIVERED_STATES = {"ENTREGADO"}
    RETURNED_STATES = {"DEVOLUCION", "DEVOLUCIÓN"}
    CANCELLED_STATES = {"CANCELADO"}
    PENDING_CONFIRMATION_STATES = {"PENDIENTE", "PENDIENTE CONFIRMACION", "PENDIENTE CONFIRMACIÓN"}
    
    stats = {
        "total": 0,
        "delivered": 0,
        "returned": 0,
        "en_ruta": 0,
        "cancelled": 0,
        "pending_confirmation": 0,
        "delivered_profit": 0,
        "pending_profit": 0
    }
    
    if orders_result.get("success"):
        orders = orders_result.get("data", {}).get("objects", [])
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        
        for order in orders:
            created_str = order.get("created_at", "")
            if not created_str:
                continue
            try:
                created_dt = datetime.strptime(created_str[:19], "%Y-%m-%dT%H:%M:%S")
                if not (start_dt <= created_dt <= end_dt):
                    continue
            except:
                continue
            
            status_raw = order.get("status", "")
            if isinstance(status_raw, dict):
                status_raw = status_raw.get("name", status_raw.get("id", "unknown"))
            status_upper = str(status_raw).upper().strip()
            
            profit = float(order.get("dropshipper_amount_to_win", 0) or 0)
            
            stats["total"] += 1
            
            if status_upper in DELIVERED_STATES:
                stats["delivered"] += 1
                stats["delivered_profit"] += profit
            elif status_upper in RETURNED_STATES:
                stats["returned"] += 1
            elif status_upper in CANCELLED_STATES:
                stats["cancelled"] += 1
            elif status_upper in PENDING_CONFIRMATION_STATES:
                stats["pending_confirmation"] += 1
            else:
                # Todo lo demás es EN_RUTA
                stats["en_ruta"] += 1
                stats["pending_profit"] += profit
    
    # Calcular tasas
    completed = stats["delivered"] + stats["returned"]
    stats["effective_delivery_rate"] = round(
        (stats["delivered"] / completed * 100) if completed > 0 else 0, 1
    )
    stats["effective_return_rate"] = round(
        (stats["returned"] / completed * 100) if completed > 0 else 0, 1
    )
    stats["cancellation_rate"] = round(
        (stats["cancelled"] / stats["total"] * 100) if stats["total"] > 0 else 0, 1
    )
    stats["total_operativo"] = stats["delivered"] + stats["returned"] + stats["en_ruta"]
    stats["completion_rate"] = round(
        (completed / stats["total_operativo"] * 100) if stats["total_operativo"] > 0 else 0, 1
    )
    
    # Conciliación
    conciliacion = {
        "entregas_orden": stats["delivered"],
        "entregas_pagadas": wallet_stats["count_ganancias"],
        "entregas_pendientes_pago": stats["delivered"] - wallet_stats["count_ganancias"],
        "devoluciones_orden": stats["returned"],
        "devoluciones_cobradas": wallet_stats["count_devoluciones"],
        "devoluciones_pendientes_cobro": stats["returned"] - wallet_stats["count_devoluciones"]
    }
    
    # Impacto pendiente
    conciliacion["dinero_por_recibir"] = conciliacion["entregas_pendientes_pago"] * wallet_stats["promedio_ganancia"]
    conciliacion["dinero_por_descontar"] = conciliacion["devoluciones_pendientes_cobro"] * wallet_stats["promedio_devolucion"]
    conciliacion["impacto_neto"] = conciliacion["dinero_por_recibir"] - conciliacion["dinero_por_descontar"]
    
    return {
        "orders": stats,
        "wallet": {
            "balance": wallet_balance,
            **wallet_stats
        },
        "conciliacion": conciliacion
    }


async def call_claude(system: str, user_message: str, api_key: str = None) -> str:
    """Llamar a Claude API"""
    if not api_key:
        return "Error: API key de Anthropic no configurada. Contacta al administrador."
    
    url = "https://api.anthropic.com/v1/messages"
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
        "anthropic-version": "2023-06-01"
    }
    payload = {
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 1024,
        "system": system,
        "messages": [{"role": "user", "content": user_message}]
    }
    
    async with httpx.AsyncClient(timeout=60) as client:
        try:
            response = await client.post(url, headers=headers, json=payload)
            if response.status_code == 200:
                data = response.json()
                content = data.get("content", [])
                if content and len(content) > 0:
                    return content[0].get("text", "No pude generar respuesta")
            elif response.status_code == 401:
                return "Error: Tu API key de Anthropic es inválida. Actualízala en Configuración."
            else:
                return f"Error API: {response.status_code}"
        except Exception as e:
            return f"Error de conexión: {str(e)}"


# ========== ENDPOINTS ==========

@router.post("/message", response_model=ChatResponse)
async def send_message(
    data: ChatMessage,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Enviar mensaje al Cerebro"""
    
    today = datetime.now()
    today_str = today.strftime("%Y-%m-%d")
    
    # Detectar período de la pregunta
    message_lower = data.message.lower()
    
    if "hoy" in message_lower:
        start_date = end_date = today_str
        period_label = "Hoy"
    elif "ayer" in message_lower:
        yesterday = today - timedelta(days=1)
        start_date = end_date = yesterday.strftime("%Y-%m-%d")
        period_label = "Ayer"
    elif "semana" in message_lower or "7 días" in message_lower or "7 dias" in message_lower:
        start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = today_str
        period_label = "Últimos 7 días"
    elif "mes" in message_lower or "30 días" in message_lower or "30 dias" in message_lower:
        start_date = (today - timedelta(days=30)).strftime("%Y-%m-%d")
        end_date = today_str
        period_label = "Últimos 30 días"
    else:
        # Default: últimos 7 días
        start_date = (today - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = today_str
        period_label = "Últimos 7 días (default)"
    
    # Recopilar datos de todas las fuentes
    user_data = {
        "periodo": period_label,
        "fecha_inicio": start_date,
        "fecha_fin": end_date,
        "meta_ads": None,
        "dropi": None,
        "lucidbot": None
    }
    
    # 1. Meta Ads
    meta_account = db.query(MetaAccount).filter(
        MetaAccount.user_id == current_user.id,
        MetaAccount.is_active == True
    ).first()
    
    if meta_account:
        try:
            meta_token = decrypt_token(meta_account.access_token_encrypted)
            user_data["meta_ads"] = await get_meta_spend(
                meta_token, meta_account.account_id, start_date, end_date
            )
        except:
            user_data["meta_ads"] = {"error": "No se pudo obtener datos de Meta"}
    else:
        user_data["meta_ads"] = {"error": "Meta Ads no conectado"}
    
    # 2. Dropi
    dropi_conn = db.query(DropiConnection).filter(
        DropiConnection.user_id == current_user.id,
        DropiConnection.is_active == True
    ).first()
    
    if dropi_conn:
        try:
            from routers.dropi import ensure_dropi_token
            dropi_token = await ensure_dropi_token(dropi_conn, db)
            dropi_data = await get_dropi_data(
                dropi_token, dropi_conn.country, start_date, end_date
            )
            user_data["dropi"] = dropi_data
        except Exception as e:
            user_data["dropi"] = {"error": f"No se pudo obtener datos de Dropi: {str(e)}"}
    else:
        user_data["dropi"] = {"error": "Dropi no conectado"}
    
    # 3. LucidBot (resumen básico)
    lucid_conn = db.query(LucidbotConnection).filter(
        LucidbotConnection.user_id == current_user.id,
        LucidbotConnection.is_active == True
    ).first()
    
    if lucid_conn:
        user_data["lucidbot"] = {"connected": True, "account_id": lucid_conn.account_id}
    else:
        user_data["lucidbot"] = {"error": "LucidBot no conectado"}
    
    # Calcular métricas combinadas si hay datos
    meta_spend = 0
    if user_data["meta_ads"] and not user_data["meta_ads"].get("error"):
        meta_spend = user_data["meta_ads"]["spend"]
    
    if user_data["dropi"] and not user_data["dropi"].get("error"):
        dropi = user_data["dropi"]
        wallet = dropi.get("wallet", {})
        orders = dropi.get("orders", {})
        
        utilidad_wallet = wallet.get("utilidad_neta", 0)
        delivered = orders.get("delivered", 0)
        
        user_data["calculado"] = {
            "profit_neto_total": utilidad_wallet - meta_spend,
            "roas": round(utilidad_wallet / meta_spend, 2) if meta_spend > 0 else 0,
            "cpa_real": round(meta_spend / delivered, 2) if delivered > 0 else 0,
            "ganando": utilidad_wallet > meta_spend
        }
    
    # Preparar system prompt con datos
    system = SYSTEM_PROMPT.format(
        today=today_str,
        user_data=json.dumps(user_data, indent=2, ensure_ascii=False, default=str)
    )
    
    # Obtener API key del usuario
    user_api_key = None
    if current_user.anthropic_api_key_encrypted:
        try:
            user_api_key = decrypt_token(current_user.anthropic_api_key_encrypted)
        except:
            pass
    
    if not user_api_key:
        return ChatResponse(
            response="⚠️ No tienes configurada tu API key de Anthropic.\n\nVe a **Configuración** → **API de Anthropic** para agregar tu key.\n\nPuedes obtener una en: https://console.anthropic.com/",
            data_used=user_data
        )
    
    # Llamar a Claude con la API key del usuario
    response_text = await call_claude(system, data.message, user_api_key)
    
    # Guardar en historial
    db.add(ChatHistory(user_id=current_user.id, role="user", content=data.message))
    db.add(ChatHistory(user_id=current_user.id, role="assistant", content=response_text))
    db.commit()
    
    return ChatResponse(
        response=response_text,
        data_used=user_data
    )


@router.get("/history")
async def get_chat_history(
    limit: int = 20,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Obtener historial de chat"""
    messages = db.query(ChatHistory).filter(
        ChatHistory.user_id == current_user.id
    ).order_by(ChatHistory.created_at.desc()).limit(limit).all()
    
    return {
        "messages": [
            {
                "role": m.role,
                "content": m.content,
                "created_at": m.created_at.isoformat()
            }
            for m in reversed(messages)
        ]
    }


@router.delete("/history")
async def clear_chat_history(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """Limpiar historial de chat"""
    db.query(ChatHistory).filter(
        ChatHistory.user_id == current_user.id
    ).delete()
    db.commit()
    
    return {"message": "Historial limpiado"}
