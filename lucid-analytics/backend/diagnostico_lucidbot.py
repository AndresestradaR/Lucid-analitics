"""
SCRIPT DE DIAGNÓSTICO - LucidBot API
=====================================
Este script NO modifica nada. Solo consulta y muestra datos para entender el problema.

Ejecutar: python diagnostico_lucidbot.py

Necesita:
- El token de LucidBot (lo puedes sacar de la base de datos o del panel)
"""

import asyncio
import httpx
from datetime import datetime

# ============ CONFIGURACIÓN ============
# PEGA TU TOKEN DE LUCIDBOT AQUÍ:
LUCIDBOT_TOKEN = "TU_TOKEN_AQUI"

# Ad ID para probar (uno que sabes tiene ventas):
TEST_AD_ID = "120236155688730647"  # El que tiene 32 ventas según tu conteo

LUCIDBOT_BASE_URL = "https://panel.lucidbot.co/api"
AD_FIELD_ID = "728462"

# ============ FUNCIONES ============

async def get_contacts_raw(token: str, ad_id: str, page: int = 1):
    """Obtener contactos crudos de la API"""
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.get(
            f"{LUCIDBOT_BASE_URL}/users/find_by_custom_field",
            headers={
                "X-ACCESS-TOKEN": token,
                "Accept": "application/json"
            },
            params={
                "field_id": AD_FIELD_ID,
                "value": ad_id,
                "page": page
            }
        )
        return response.status_code, response.json() if response.status_code == 200 else response.text


async def get_all_contacts(token: str, ad_id: str):
    """Obtener TODOS los contactos con paginación"""
    all_contacts = []
    page = 1
    max_pages = 20
    
    while page <= max_pages:
        status, data = await get_contacts_raw(token, ad_id, page)
        
        if status != 200:
            print(f"  ERROR en página {page}: {data}")
            break
        
        contacts = data.get("data", [])
        print(f"  Página {page}: {len(contacts)} contactos")
        
        if not contacts:
            break
        
        all_contacts.extend(contacts)
        
        if len(contacts) < 100:
            break
        
        page += 1
    
    return all_contacts


def analyze_contacts(contacts: list, start_date: str, end_date: str):
    """Analizar contactos y contar ventas"""
    
    results = {
        "total_contactos": len(contacts),
        "con_total_pagar": 0,
        "sin_total_pagar": 0,
        "en_rango_fecha": 0,
        "fuera_rango_fecha": 0,
        "sin_fecha": 0,
        "ventas_en_rango": 0,
        "fechas_encontradas": [],
        "ejemplos_filtrados": [],
        "ejemplos_ventas": []
    }
    
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    
    for contact in contacts:
        custom_fields = contact.get("custom_fields", {})
        created_at = contact.get("created_at", "")
        total_pagar = custom_fields.get("Total a pagar")
        
        # Contar Total a pagar
        if total_pagar:
            results["con_total_pagar"] += 1
        else:
            results["sin_total_pagar"] += 1
        
        # Analizar fecha
        if not created_at:
            results["sin_fecha"] += 1
            continue
        
        try:
            # Parsear fecha
            if " " in created_at:
                contact_date = datetime.strptime(created_at.split(" ")[0], "%Y-%m-%d").date()
            elif "T" in created_at:
                contact_date = datetime.strptime(created_at.split("T")[0], "%Y-%m-%d").date()
            else:
                contact_date = datetime.strptime(created_at, "%Y-%m-%d").date()
            
            # Guardar fecha para análisis
            fecha_str = contact_date.strftime("%Y-%m-%d")
            if fecha_str not in results["fechas_encontradas"]:
                results["fechas_encontradas"].append(fecha_str)
            
            # Verificar si está en rango
            if start <= contact_date <= end:
                results["en_rango_fecha"] += 1
                
                # Si tiene Total a pagar, es venta
                if total_pagar:
                    results["ventas_en_rango"] += 1
                    if len(results["ejemplos_ventas"]) < 5:
                        results["ejemplos_ventas"].append({
                            "nombre": contact.get("full_name", "")[:30],
                            "fecha": created_at,
                            "total_pagar": total_pagar,
                            "calificacion": custom_fields.get("Calificacion_LucidSales", "")
                        })
            else:
                results["fuera_rango_fecha"] += 1
                if len(results["ejemplos_filtrados"]) < 5:
                    results["ejemplos_filtrados"].append({
                        "fecha": created_at,
                        "rango": f"{start_date} a {end_date}",
                        "total_pagar": total_pagar
                    })
                    
        except Exception as e:
            results["sin_fecha"] += 1
    
    # Ordenar fechas
    results["fechas_encontradas"].sort()
    
    return results


async def main():
    print("=" * 60)
    print("DIAGNÓSTICO LUCIDBOT API")
    print("=" * 60)
    
    if LUCIDBOT_TOKEN == "TU_TOKEN_AQUI":
        print("\n❌ ERROR: Necesitas pegar tu token de LucidBot en el script")
        print("   Edita la línea: LUCIDBOT_TOKEN = 'TU_TOKEN_AQUI'")
        return
    
    print(f"\nAd ID a probar: {TEST_AD_ID}")
    
    # TEST 1: Obtener todos los contactos
    print("\n" + "-" * 40)
    print("TEST 1: Obteniendo TODOS los contactos...")
    print("-" * 40)
    
    all_contacts = await get_all_contacts(LUCIDBOT_TOKEN, TEST_AD_ID)
    print(f"\n📊 Total contactos obtenidos: {len(all_contacts)}")
    
    if not all_contacts:
        print("❌ No se obtuvieron contactos. Verifica el token y el Ad ID.")
        return
    
    # TEST 2: Análisis para un solo día (13 dic)
    print("\n" + "-" * 40)
    print("TEST 2: Análisis para UN SOLO DÍA (13 dic)")
    print("-" * 40)
    
    results_dia = analyze_contacts(all_contacts, "2025-12-13", "2025-12-13")
    print(f"  Total contactos: {results_dia['total_contactos']}")
    print(f"  Con 'Total a pagar': {results_dia['con_total_pagar']}")
    print(f"  En rango de fecha: {results_dia['en_rango_fecha']}")
    print(f"  ⭐ VENTAS en rango: {results_dia['ventas_en_rango']}")
    
    # TEST 3: Análisis para semana (1-7 dic)
    print("\n" + "-" * 40)
    print("TEST 3: Análisis para SEMANA (1-7 dic)")
    print("-" * 40)
    
    results_semana = analyze_contacts(all_contacts, "2025-12-01", "2025-12-07")
    print(f"  Total contactos: {results_semana['total_contactos']}")
    print(f"  Con 'Total a pagar': {results_semana['con_total_pagar']}")
    print(f"  En rango de fecha: {results_semana['en_rango_fecha']}")
    print(f"  ⭐ VENTAS en rango: {results_semana['ventas_en_rango']}")
    
    # TEST 4: Distribución de fechas
    print("\n" + "-" * 40)
    print("TEST 4: Distribución de fechas de contactos")
    print("-" * 40)
    print(f"  Fechas encontradas: {results_semana['fechas_encontradas']}")
    
    # TEST 5: Ejemplos de contactos filtrados
    if results_semana['ejemplos_filtrados']:
        print("\n" + "-" * 40)
        print("TEST 5: Ejemplos de contactos FILTRADOS (fuera de rango)")
        print("-" * 40)
        for ej in results_semana['ejemplos_filtrados']:
            print(f"  Fecha: {ej['fecha']} | Rango: {ej['rango']} | Total a pagar: {ej['total_pagar']}")
    
    # TEST 6: Ejemplos de ventas en rango
    if results_semana['ejemplos_ventas']:
        print("\n" + "-" * 40)
        print("TEST 6: Ejemplos de VENTAS en rango")
        print("-" * 40)
        for ej in results_semana['ejemplos_ventas']:
            print(f"  {ej['nombre']} | {ej['fecha']} | ${ej['total_pagar']} | {ej['calificacion']}")
    
    # RESUMEN FINAL
    print("\n" + "=" * 60)
    print("RESUMEN COMPARATIVO")
    print("=" * 60)
    print(f"\n  {'Métrica':<30} {'1 día (13dic)':<15} {'Semana (1-7dic)':<15}")
    print(f"  {'-'*30} {'-'*15} {'-'*15}")
    print(f"  {'Contactos totales':<30} {results_dia['total_contactos']:<15} {results_semana['total_contactos']:<15}")
    print(f"  {'En rango de fecha':<30} {results_dia['en_rango_fecha']:<15} {results_semana['en_rango_fecha']:<15}")
    print(f"  {'VENTAS en rango':<30} {results_dia['ventas_en_rango']:<15} {results_semana['ventas_en_rango']:<15}")
    print(f"  {'Fuera de rango':<30} {results_dia['fuera_rango_fecha']:<15} {results_semana['fuera_rango_fecha']:<15}")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
