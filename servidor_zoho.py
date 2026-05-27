import os
import json
import requests
import asyncio
import uuid
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import Dict, Any

# 1. CONFIGURACIÓN DE ZOHO
ZOHO_CLIENT_ID = os.environ["ZOHO_CLIENT_ID"]
ZOHO_CLIENT_SECRET = os.environ["ZOHO_CLIENT_SECRET"]
ZOHO_REFRESH_TOKEN = os.environ["ZOHO_REFRESH_TOKEN"]
ZOHO_ORG_ID = os.environ["ZOHO_ORG_ID"]
ZOHO_API_DOMAIN = os.environ.get("ZOHO_API_DOMAIN", "https://www.zohoapis.eu")

# 2. OBTENER ACCESS TOKEN
def get_access_token():
    """Obtiene un access_token usando el refresh_token."""
    url = "https://accounts.zoho.eu/oauth/v2/token"
    params = {
        "refresh_token": ZOHO_REFRESH_TOKEN,
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "grant_type": "refresh_token"
    }
    response = requests.post(url, params=params)
    return response.json().get("access_token")

# 3. LLAMADA GENÉRICA A LA API DE ZOHO
def zoho_api(endpoint, params=None):
    """Hace una llamada GET a la API de Zoho Inventory."""
    token = get_access_token()
    headers = {
        "Authorization": f"Zoho-oauthtoken {token}",
        "orgId": ZOHO_ORG_ID
    }
    url = f"{ZOHO_API_DOMAIN}/inventory/v1/{endpoint}"
    response = requests.get(url, headers=headers, params=params)
    return response.json()

# 4. APP FASTAPI
app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# 5. HERRAMIENTAS DISPONIBLES
TOOLS = {
    "tools": [
        {
            "name": "buscar_productos",
            "description": "Busca productos en Zoho Inventory por nombre o SKU.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "keyword": {"type": "string", "description": "Palabra clave para buscar por nombre."},
                    "sku": {"type": "string", "description": "SKU del producto."},
                    "limite": {"type": "integer", "description": "Máximo de productos.", "default": 20}
                }
            }
        },
        {
            "name": "consultar_stock",
            "description": "Consulta el stock detallado de un producto por su SKU exacto.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sku": {"type": "string", "description": "SKU exacto del producto."}
                }
            }
        },
        {
            "name": "productos_stock_bajo",
            "description": "Lista productos con stock bajo (menos de 10 unidades).",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limite": {"type": "integer", "default": 50}
                }
            }
        },
        {
            "name": "resumen_inventario",
            "description": "Muestra resumen general del inventario.",
            "inputSchema": {"type": "object", "properties": {}}
        }
    ]
}

# 6. COLA DE MENSAJES POR SESIÓN
sessions: Dict[str, asyncio.Queue] = {}

# 7. ENDPOINT SSE
@app.get("/sse")
async def sse(request: Request):
    session_id = request.query_params.get("session_id", str(uuid.uuid4()))
    queue: asyncio.Queue = asyncio.Queue()
    sessions[session_id] = queue
    
    async def generator():
        try:
            yield f"event: endpoint\ndata: /messages?session_id={session_id}\n\n"
            while True:
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=15)
                    yield f"event: message\ndata: {message}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            sessions.pop(session_id, None)
    
    return StreamingResponse(generator(), media_type="text/event-stream",
                           headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

# 8. ENDPOINT MENSAJES
@app.post("/messages")
async def messages(request: Request):
    body = await request.json()
    session_id = request.query_params.get("session_id", "")
    
    method = body.get("method", "")
    msg_id = body.get("id", None)
    
    if msg_id is None:
        return {}
    
    respuesta = None
    
    if method == "initialize":
        respuesta = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": "2025-06-18",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "Zoho Inventory Server", "version": "1.0.0"}
            }
        }
    
    elif method == "tools/list":
        respuesta = {"jsonrpc": "2.0", "id": msg_id, "result": TOOLS}
    
    elif method == "tools/call":
        params = body.get("params", {})
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})
        
        if tool_name == "buscar_productos":
            keyword = arguments.get("keyword", "")
            sku = arguments.get("sku", "")
            limite = arguments.get("limite", 20)
            
            data = zoho_api("items", params={"organization_id": ZOHO_ORG_ID})
            
            if "items" in data:
                items = data["items"]
                if keyword:
                    items = [i for i in items if keyword.lower() in i.get("name", "").lower()]
                if sku:
                    items = [i for i in items if sku.lower() in i.get("sku", "").lower()]
                results = items[:limite]
            else:
                results = {"error": "No se pudieron obtener productos"}
        
        elif tool_name == "consultar_stock":
            sku = arguments.get("sku", "")
            data = zoho_api(f"items?sku={sku}")
            
            if "items" in data and len(data["items"]) > 0:
                item = data["items"][0]
                results = [{
                    "name": item.get("name"),
                    "sku": item.get("sku"),
                    "stock": item.get("stock_on_hand"),
                    "precio": item.get("rate")
                }]
            else:
                results = {"error": f"No se encontró el SKU: {sku}"}
        
        elif tool_name == "productos_stock_bajo":
            limite = arguments.get("limite", 50)
            data = zoho_api("items", params={"organization_id": ZOHO_ORG_ID})
            
            if "items" in data:
                items = [i for i in data["items"] if i.get("stock_on_hand", 0) < 10]
                results = items[:limite]
            else:
                results = {"error": "No se pudieron obtener productos"}
        
        elif tool_name == "resumen_inventario":
            data = zoho_api("items", params={"organization_id": ZOHO_ORG_ID})
            
            if "items" in data:
                items = data["items"]
                total_productos = len(items)
                total_stock = sum(i.get("stock_on_hand", 0) for i in items)
                valor_total = sum(i.get("stock_on_hand", 0) * i.get("rate", 0) for i in items)
                
                results = {
                    "total_productos": total_productos,
                    "total_stock": total_stock,
                    "valor_total_inventario": round(valor_total, 2)
                }
            else:
                results = {"error": "No se pudieron obtener datos"}
        
        else:
            results = {"error": f"Herramienta no encontrada: {tool_name}"}
        
        respuesta = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "content": [{"type": "text", "text": json.dumps(results, indent=2, ensure_ascii=False)}]
            }
        }
    
    else:
        respuesta = {"jsonrpc": "2.0", "id": msg_id, "error": {"code": -32601, "message": "Method not found"}}
    
    respuesta_str = json.dumps(respuesta)
    if session_id in sessions:
        await sessions[session_id].put(respuesta_str)
    
    return {"status": "ok"}

@app.get("/")
def root():
    return {"status": "ok", "total_tools": len(TOOLS["tools"])}ç