# Lucid Analytics - Backend API

Dashboard de métricas para dropshipping COD que integra **Meta Ads + LucidBot** para calcular CPA real por anuncio.

## 🚀 Características

- **Multi-usuario**: Cada usuario conecta sus propias cuentas
- **Meta Ads OAuth**: Conexión segura con múltiples cuentas de Meta
- **LucidBot Integration**: Obtiene leads y ventas por anuncio
- **Métricas Calculadas**: CPA, ROAS, CPL, CTR, CPM por anuncio
- **Dashboard**: Resumen general + Top/Worst anuncios

## 📊 Endpoints principales

### Auth
- `POST /api/auth/register` - Registrar usuario
- `POST /api/auth/login` - Iniciar sesión
- `GET /api/auth/me` - Usuario actual

### Meta Ads
- `GET /api/meta/auth-url` - Obtener URL de OAuth
- `POST /api/meta/callback` - Callback de OAuth
- `GET /api/meta/accounts` - Listar cuentas conectadas
- `GET /api/meta/ads` - Métricas de anuncios

### LucidBot
- `POST /api/lucidbot/connect` - Conectar con token
- `GET /api/lucidbot/contacts/by-ad/{ad_id}` - Contactos por anuncio
- `GET /api/lucidbot/all-ad-ids` - Todos los Ad IDs

### Analytics (el importante)
- `GET /api/analytics/dashboard` - Dashboard completo
- `GET /api/analytics/ad/{ad_id}` - Detalle de un anuncio
- `GET /api/analytics/chart/daily` - Datos para gráfico diario
- `GET /api/analytics/compare-ads` - Comparar anuncios

## 🛠️ Instalación local

```bash
# Clonar repo
git clone https://github.com/tu-usuario/lucid-analytics.git
cd lucid-analytics/backend

# Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# o: venv\Scripts\activate  # Windows

# Instalar dependencias
pip install -r requirements.txt

# Copiar variables de entorno
cp .env.example .env
# Editar .env con tus valores

# Ejecutar
uvicorn main:app --reload
```

## 🚀 Deploy en Railway

1. Crear nuevo proyecto en Railway
2. Conectar repositorio de GitHub
3. Agregar servicio de PostgreSQL
4. Configurar variables de entorno:
   - `SECRET_KEY` (generar uno seguro)
   - `ENCRYPTION_KEY` (generar uno seguro)
   - `META_APP_ID`
   - `META_APP_SECRET`
5. Deploy automático

## 🔐 Configurar Meta App

1. Ir a https://developers.facebook.com/apps/
2. Crear nueva app → "Business" → "Business"
3. Agregar producto "Facebook Login"
4. En Settings > Basic:
   - Copiar App ID → `META_APP_ID`
   - Copiar App Secret → `META_APP_SECRET`
5. En Facebook Login > Settings:
   - Agregar tu dominio a "Valid OAuth Redirect URIs"
   - Ej: `https://tu-frontend.vercel.app/callback`

## 📁 Estructura

```
backend/
├── main.py              # FastAPI app
├── database.py          # Modelos SQLAlchemy
├── schemas.py           # Schemas Pydantic
├── utils.py             # JWT y encriptación
├── routers/
│   ├── auth.py          # Autenticación
│   ├── meta.py          # Meta Ads API
│   ├── lucidbot.py      # LucidBot API
│   └── analytics.py     # Métricas combinadas
├── requirements.txt
├── Procfile
└── .env.example
```

## 🔑 Variables de entorno

| Variable | Descripción |
|----------|-------------|
| `DATABASE_URL` | URL de PostgreSQL |
| `SECRET_KEY` | Clave para JWT (32+ caracteres) |
| `ENCRYPTION_KEY` | Clave Fernet para encriptar tokens |
| `META_APP_ID` | App ID de Meta |
| `META_APP_SECRET` | App Secret de Meta |

## 🧪 Generar claves seguras

```python
# Para SECRET_KEY
import secrets
print(secrets.token_urlsafe(32))

# Para ENCRYPTION_KEY
from cryptography.fernet import Fernet
print(Fernet.generate_key().decode())
```

## 📝 Notas importantes

- Los tokens de Meta y LucidBot se guardan **encriptados** en la DB
- El OAuth de Meta requiere dominio HTTPS en producción
- LucidBot usa el campo "Anuncio Facebook" (ID: 728462) para el Ad ID
- El campo "Total a pagar" indica que un contacto es una venta

## 🤝 Soporte

Para problemas o preguntas, abrir un Issue en GitHub.
