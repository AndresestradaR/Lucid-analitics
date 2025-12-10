"""
Pydantic schemas para validación de datos
"""

from pydantic import BaseModel, EmailStr
from typing import Optional, List
from datetime import datetime


# ========== AUTH ==========

class UserCreate(BaseModel):
    email: EmailStr
    password: str
    name: Optional[str] = None

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class UserResponse(BaseModel):
    id: int
    email: str
    name: Optional[str]
    is_active: bool
    is_admin: bool
    created_at: datetime
    
    class Config:
        from_attributes = True

class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserResponse


# ========== META ==========

class MetaConnectRequest(BaseModel):
    """Para iniciar OAuth de Meta"""
    redirect_uri: str

class MetaCallbackRequest(BaseModel):
    """Callback de Meta OAuth"""
    code: str
    redirect_uri: str

class MetaAccountResponse(BaseModel):
    id: int
    account_id: str
    account_name: Optional[str]
    is_active: bool
    created_at: datetime
    
    class Config:
        from_attributes = True

class MetaAdMetrics(BaseModel):
    """Métricas de un anuncio de Meta"""
    ad_id: str
    ad_name: Optional[str]
    spend: float
    impressions: int
    clicks: int
    ctr: float
    cpm: float
    cpc: float
    reach: int


# ========== LUCIDBOT ==========

class LucidbotConnectRequest(BaseModel):
    """Conectar LucidBot"""
    api_token: str

class LucidbotConnectionResponse(BaseModel):
    id: int
    account_id: Optional[str]
    is_active: bool
    created_at: datetime
    
    class Config:
        from_attributes = True

class LucidbotContact(BaseModel):
    """Contacto de LucidBot"""
    id: str
    name: str
    phone: Optional[str]
    ad_id: Optional[str]
    total_paid: Optional[float]
    created_at: Optional[str]


# ========== ANALYTICS ==========

class DateRangeRequest(BaseModel):
    """Rango de fechas para consultas"""
    start_date: str  # YYYY-MM-DD
    end_date: str    # YYYY-MM-DD
    account_id: Optional[str] = None  # Meta Ad Account ID

class AdAnalytics(BaseModel):
    """Métricas completas de un anuncio"""
    ad_id: str
    ad_name: Optional[str]
    
    # Métricas de Meta
    spend: float
    impressions: int
    clicks: int
    ctr: float
    cpm: float
    
    # Métricas de LucidBot
    leads: int
    sales: int
    revenue: float
    
    # Métricas calculadas
    cpl: float  # Cost per Lead
    cpa: float  # Cost per Acquisition (Sale)
    roas: float  # Return on Ad Spend

class DashboardSummary(BaseModel):
    """Resumen general del dashboard"""
    total_spend: float
    total_revenue: float
    total_leads: int
    total_sales: int
    average_cpa: float
    average_roas: float
    conversion_rate: float  # Sales / Leads
    
    top_ads: List[AdAnalytics]
    worst_ads: List[AdAnalytics]

class ChartDataPoint(BaseModel):
    """Punto de datos para gráficos"""
    date: str
    spend: float
    revenue: float
    cpa: float
    roas: float
    leads: int
    sales: int
