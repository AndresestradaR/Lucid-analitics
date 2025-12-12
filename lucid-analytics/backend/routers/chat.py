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


class ChatResponse(BaseModel):
    response: str
    data_used: Optional[dict] = None


# ========== SYSTEM PROMPT ==========

SYSTEM_PROMPT = """Eres "El Cerebro", el asistente financiero de un negocio de Dropshipping.
Hoy es {today}.

## TU MISIÓN
Ayudar al dueño a entender si está GANANDO o PERDIENDO dinero con análisis claros.

## DATOS DISPONIBLES
Te pasaré datos de 3 fuentes:

### META ADS (Publicidad)
- Gasto en campañas
- CPA, CTR, CPM
- Leads generados

### LUCIDBOT (CRM WhatsApp)
- Leads por anuncio
- Ventas confirmadas
- Revenue

### DROPI (Logística)
- Pedidos: entregados, devueltos, pendientes
- Profit por pedido entregado
- Costo de devoluciones (Q23,000 COP c/u)
- Saldo en wallet

## MÉTRICAS CLAVE

### CPA (Costo Por Adquisición)
- CPA Inicial = Gasto Ads ÷ Pedidos totales
- CPA Real = Gasto Ads ÷ Pedidos ENTREGADOS

### Profit
- Profit Bruto = Ganancia Dropi (entregados)
- Profit Neto = Profit Bruto - Gasto Ads - Costo devoluciones
- ✅ Positivo = Ganando
- ❌ Negativo = Perdiendo

### ROAS
- ROAS = Revenue ÷ Gasto Ads
- > 2 es bueno, > 3 excelente

## ESTILO
- Responde en ESPAÑOL
- Sé directo y conciso
- Usa emojis con moderación
- Siempre da un veredicto claro
- Si faltan datos, indícalo

## DATOS DEL USUARIO
{user_data}

Analiza los datos y responde la pregunta del usuario.
"""


# ========== HELPERS ==========

async def get_meta_spend(token: str, account_id: str, start_date: str, end_date: str) -> dict:
    """Obtener gasto de Meta Ads"""
    url = f"https://graph.facebook.com/v21.0/act_{account_id}/insights"
    params = {
        "access_token": token,
        "level": "account",
        "fields": "spend,impressions,clicks,ctr,cpm",
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
    """Obtener datos de Dropi"""
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
    
    # Obtener órdenes
    orders_result = await dropi_request(
        "GET", "/api/orders/myorders", token, country,
        params={"result_number": 500, "order_by": "created_at", "order_dir": "desc"}
    )
    
    stats = {
        "total": 0, "delivered": 0, "returned": 0, "pending": 0,
        "delivered_profit": 0, "pending_profit": 0, "return_cost": 0
    }
    
    if orders_result.get("success"):
        orders = orders_result.get("data", {}).get("objects", [])
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)
        
        # STATUS_MAP para Colombia (strings en español)
        STATUS_MAP = {
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
            "NOVEDAD": "shipped",
            "EN BODEGA": "confirmed",
            # Numéricos por si acaso
            1: "pending", 2: "confirmed", 3: "shipped",
            4: "delivered", 5: "returned", 6: "cancelled",
        }
        
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
            
            # Extraer status (puede ser string o dict)
            status_raw = order.get("status", "")
            if isinstance(status_raw, dict):
                status_raw = status_raw.get("name", status_raw.get("id", "unknown"))
            status_upper = str(status_raw).upper().strip()
            status_name = STATUS_MAP.get(status_upper, STATUS_MAP.get(status_raw, "unknown"))
            
            profit = float(order.get("dropshipper_amount_to_win", 0) or 0)
            
            stats["total"] += 1
            
            if status_name == "delivered":
                stats["delivered"] += 1
                stats["delivered_profit"] += profit
            elif status_name == "returned":
                stats["returned"] += 1
                stats["return_cost"] += 23000
            elif status_name in ["pending", "confirmed", "shipped"]:
                stats["pending"] += 1
                stats["pending_profit"] += profit
    
    stats["net_profit"] = stats["delivered_profit"] - stats["return_cost"]
    stats["wallet_balance"] = wallet_balance
    
    return stats


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
            # Asegurar token válido
            from routers.dropi import ensure_dropi_token
            dropi_token = await ensure_dropi_token(dropi_conn, db)
            user_data["dropi"] = await get_dropi_data(
                dropi_token, dropi_conn.country, start_date, end_date
            )
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
    if user_data["meta_ads"] and not user_data["meta_ads"].get("error"):
        if user_data["dropi"] and not user_data["dropi"].get("error"):
            meta_spend = user_data["meta_ads"]["spend"]
            dropi_profit = user_data["dropi"]["net_profit"]
            
            user_data["calculado"] = {
                "profit_neto": dropi_profit - meta_spend,
                "roas": round(dropi_profit / meta_spend, 2) if meta_spend > 0 else 0,
                "cpa_real": round(meta_spend / user_data["dropi"]["delivered"], 2) if user_data["dropi"]["delivered"] > 0 else 0
            }
    
    # Preparar system prompt con datos
    system = SYSTEM_PROMPT.format(
        today=today_str,
        user_data=json.dumps(user_data, indent=2, ensure_ascii=False)
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
