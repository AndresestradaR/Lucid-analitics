---
name: lucid-analytics
description: |
  Documentación técnica de Lucid Analytics - SaaS de Business Intelligence para dropshipping.
  Integra Meta Ads, LucidBot (WhatsApp CRM) y Dropi (fulfillment) para calcular CPA y ROAS real.
  
  USAR ESTE SKILL CUANDO:
  - Se va a modificar código del backend de Lucid Analytics
  - Se necesita debuggear problemas de datos ($0 en dashboard, sync fallidos)
  - Se va a crear una integración similar con Meta/LucidBot/Dropi
  - Se necesita entender cómo fluyen los datos entre sistemas
  
  CRÍTICO: Leer antes de tocar sync.py, analytics.py, dropi.py o sync_dropi.py
---

# Lucid Analytics - Documentación Técnica

## Arquitectura General

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   META ADS      │     │   LUCIDBOT      │     │     DROPI       │
│   (Gasto)       │     │  (Leads/Ventas) │     │  (Fulfillment)  │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         │ OAuth Token           │ JWT Token             │ Email/Pass
         │                       │                       │
         ▼                       ▼                       ▼
┌────────────────────────────────────────────────────────────────────┐
│                      LUCID ANALYTICS BACKEND                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐              │
│  │ analytics.py │  │   sync.py    │  │ sync_dropi.py│              │
│  │ (Dashboard)  │  │  (LucidBot)  │  │   (Dropi)    │              │
│  └──────────────┘  └──────────────┘  └──────────────┘              │
│                              │                                      │
│                              ▼                                      │
│                    ┌──────────────────┐                            │
│                    │   PostgreSQL     │                            │
│                    │ (Cache + Datos)  │                            │
│                    └──────────────────┘                            │
└────────────────────────────────────────────────────────────────────┘
```

## FLUJO CRÍTICO: Correlación Meta ↔ LucidBot

**Este es el flujo más importante. Si se rompe, el dashboard muestra $0.**

```
1. META ADS devuelve:
   - ad_id: "120237294955200647"
   - spend: 50000
   - impressions, clicks, etc.

2. LUCIDBOT tiene contactos con:
   - Campo 728462 (Anuncio Facebook): "120237294955200647"  ← ad_id directo
   - Campo 764700 (JSON pedido): {"ad": "120237294955200647", "total": 59900}

3. SYNC.PY debe:
   - Obtener lista de contactos
   - Para CADA contacto, llamar a user.php con ms_id para obtener custom_fields
   - Extraer ad_id del campo 728462 O del JSON en 764700
   - Guardar contacto CON ad_id en PostgreSQL

4. ANALYTICS.PY correlaciona:
   - Busca contactos WHERE ad_id = "120237294955200647"
   - Suma leads, sales, revenue
   - Calcula CPA = spend / sales
   - Calcula ROAS = revenue / spend
```

**SI FALLA EL PASO 3 → Contactos sin ad_id → Dashboard $0**

## Archivos Críticos y Qué Hacen

### sync.py (LucidBot)
**Función:** Sincronizar contactos de LucidBot a PostgreSQL CON ad_id

**Funciones críticas:**
- `fetch_contact_custom_fields()` - Obtiene ad_id de campos 728462/764700
- `enrich_contacts_with_ad_id()` - Enriquece contactos en paralelo
- `sync_contacts_to_db()` - Guarda en BD con UPSERT

**NO TOCAR SIN ENTENDER:**
- El ad_id viene de custom_fields, NO del listado básico de contactos
- Se necesita una llamada adicional por contacto para obtenerlo

### analytics.py (Dashboard)
**Función:** Combinar datos Meta + LucidBot para mostrar métricas

**Funciones críticas:**
- `get_meta_ads_with_hierarchy()` - Obtiene spend de Meta API
- `get_lucidbot_data_batch()` - Query batch a PostgreSQL por ad_ids

**NO TOCAR SIN ENTENDER:**
- La correlación es por ad_id: `WHERE ad_id IN (lista_de_meta)`
- Si ad_id es NULL, no hay correlación

### sync_dropi.py (Dropi)
**Función:** Sincronizar órdenes y wallet de Dropi a PostgreSQL

**Funciones críticas:**
- `sync_dropi_orders()` - Pagina y guarda órdenes
- `sync_dropi_wallet()` - Pagina y guarda movimientos wallet
- `reconcile_orders_with_wallet()` - Cruza órdenes con pagos

**NO TOCAR SIN ENTENDER:**
- Login requiere headers anti-bot específicos
- El balance viene de wallets[0].amount, NO del campo balance

### dropi.py (Endpoints)
**Función:** Endpoints REST para consultar datos de Dropi desde cache

**NO TOCAR SIN ENTENDER:**
- Lee de PostgreSQL, NO de la API de Dropi
- El sync_dropi.py llena la BD, dropi.py la lee

## IDs de Campos LucidBot

| Campo ID | Nombre | Contenido |
|----------|--------|-----------||
| 728462 | Anuncio Facebook | ad_id directo (ej: "120237294955200647") |
| 764700 | JSON Pedido | {"ad": "...", "total": 59900, "products": [...]} |
| 926799 | Estado | ENTREGADO, CANCELADO, etc. |
| 117867 | Total a pagar | Valor numérico |
| 116501 | Producto | Nombre del producto |

## Checklist Antes de Modificar Código

### Si vas a tocar sync.py:
- [ ] ¿El cambio afecta cómo se extrae ad_id?
- [ ] ¿Se sigue llamando a fetch_contact_custom_fields() para cada contacto?
- [ ] ¿El ad_id se guarda en la BD?
- [ ] Después del deploy, verificar con: `GET /api/sync/lucidbot/status`

### Si vas a tocar analytics.py:
- [ ] ¿El cambio afecta la query que filtra por ad_id?
- [ ] ¿Se mantiene el batch query (no N+1)?
- [ ] Después del deploy, verificar que el dashboard muestre datos

### Si vas a tocar sync_dropi.py:
- [ ] ¿El cambio afecta el login o headers?
- [ ] ¿Se mantiene la paginación correcta?
- [ ] ¿El wallet se obtiene de wallets[0].amount?
- [ ] Después del deploy, verificar con: `GET /api/admin/dropi/sync-status`

### Si vas a tocar dropi.py:
- [ ] ¿Se sigue leyendo de PostgreSQL (no API directa)?
- [ ] ¿El cambio afecta cómo se calculan totales?

## Endpoints de Debug

| Endpoint | Qué muestra |
|----------|-------------|
| GET /api/sync/lucidbot/status | Contactos totales, con ad_id, porcentaje |
| GET /api/admin/debug/ad-ids/{user_id} | Cuántos contactos tienen ad_id |
| GET /api/admin/debug/sample-contacts/{user_id} | Muestra 5 contactos con sus datos |
| GET /api/admin/debug/lucidbot-raw/{user_id} | Respuesta cruda de LucidBot API |
| GET /api/admin/dropi/sync-status | Estado del sync de Dropi por usuario |
| GET /api/admin/users | Lista todos los usuarios con sus conexiones |

## Errores Comunes y Soluciones

### Dashboard muestra $0
**Causa:** Contactos sin ad_id
**Verificar:** GET /api/sync/lucidbot/status → ver % con ad_id
**Solución:** Revisar sync.py, asegurar que fetch_contact_custom_fields() se llama

### Dropi wallet incorrecto
**Causa:** Se está leyendo campo equivocado
**Verificar:** Logs de `[DROPI DEBUG] wallet from`
**Solución:** Leer de wallets[0].amount, no de balance

### Sync de LucidBot falla
**Causa:** Token JWT expirado
**Verificar:** Respuesta "Token inválido o expirado"
**Solución:** Usuario debe reconectar LucidBot en el frontend

### Sync de Dropi timeout
**Causa:** Headers anti-bot insuficientes
**Solución:** Ver sync_dropi.py, función get_browser_headers()

## Repositorios

| Repo | Contenido |
|------|-----------||
| Lucid-analitics | Backend FastAPI (path: lucid-analytics/backend/) |
| lucid-analytics-frontend | Frontend React + Vite |
| dropi-mcp | MCP Server para Dropi (npm: dropi-mcp) |

## Deployment

- **Backend:** Railway (auto-deploy desde main)
- **Frontend:** Vercel
- **BD:** PostgreSQL en Railway
- **Dominio API:** api.lucidestrategasia.online
