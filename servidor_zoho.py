import os
import json
import requests
import asyncio
import uuid
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from typing import Dict, Any

# CONFIGURACIÓN
ZOHO_CLIENT_ID = os.environ["ZOHO_CLIENT_ID"]
ZOHO_CLIENT_SECRET = os.environ["ZOHO_CLIENT_SECRET"]
ZOHO_REFRESH_TOKEN = os.environ["ZOHO_REFRESH_TOKEN"]
ZOHO_ORG_ID = os.environ["ZOHO_ORG_ID"]
ZOHO_API = os.environ.get("ZOHO_API_DOMAIN", "https://www.zohoapis.eu")

def get_token():
    try:
        r = requests.post("https://accounts.zoho.eu/oauth/v2/token", params={
            "refresh_token": ZOHO_REFRESH_TOKEN,
            "client_id": ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET,
            "grant_type": "refresh_token"
        })
        token = r.json()["access_token"]
        print(f"TOKEN OK: {token[:20]}...")
        return token
    except Exception as e:
        print(f"ERROR TOKEN: {str(e)}")
        raise

def get_items():
    try:
        token = get_token()
        r = requests.get(f"{ZOHO_API}/inventory/v1/items", headers={
            "Authorization": f"Zoho-oauthtoken {token}",
            "orgId": ZOHO_ORG_ID
        }, params={"organization_id": ZOHO_ORG_ID})
        data = r.json()
        items = data.get("items", [])
        print(f"ITEMS OBTENIDOS: {len(items)}")
        if items:
            print(f"PRIMER ITEM: {items[0].get('name')} - {items[0].get('sku')}")
        return items
    except Exception as e:
        print(f"ERROR get_items: {str(e)}")
        return []

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

TOOLS = {
    "tools": [
        {
            "name": "buscar_productos",
            "description": "Busca productos por nombre o SKU.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "keyword": {"type": "string"},
                    "sku": {"type": "string"},
                    "limite": {"type": "integer", "default": 20}
                }
            }
        },
        {
            "name": "consultar_stock",
            "description": "Stock de un producto por SKU exacto.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sku": {"type": "string"}
                }
            }
        },
        {
            "name": "productos_stock_bajo",
            "description": "Productos con menos de 10 unidades.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limite": {"type": "integer", "default": 50}
                }
            }
        },
        {
            "name": "resumen_inventario",
            "description": "Resumen del inventario.",
            "inputSchema": {"type": "object", "properties": {}}
        }
    ]
}

sessions: Dict[str, asyncio.Queue] = {}

@app.get("/sse")
async def sse(request: Request):
    sid = request.query_params.get("session_id", str(uuid.uuid4()))
    q = asyncio.Queue()
    sessions[sid] = q
    async def gen():
        try:
            yield f"event: endpoint\ndata: /messages?session_id={sid}\n\n"
            while True:
                try:
                    m = await asyncio.wait_for(q.get(), timeout=15)
                    yield f"event: message\ndata: {m}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            sessions.pop(sid, None)
    return StreamingResponse(gen(), media_type="text/event-stream",
                           headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.post("/messages")
async def messages(request: Request):
    body = await request.json()
    sid = request.query_params.get("session_id", "")
    method = body.get("method", "")
    mid = body.get("id", None)
    
    if mid is None:
        return {}
    
    r = None
    
    if method == "initialize":
        r = {"jsonrpc": "2.0", "id": mid, "result": {
            "protocolVersion": "2025-06-18",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "Zoho Inventory", "version": "1.0.0"}
        }}
    elif method == "tools/list":
        r = {"jsonrpc": "2.0", "id": mid, "result": TOOLS}
    elif method == "tools/call":
        args = body.get("params", {}).get("arguments", {})
        name = body.get("params", {}).get("name", "").replace("mcp_", "")
        
        print(f"HERRAMIENTA: {name}, ARGS: {args}")
        
        try:
            items = get_items()
        except Exception as e:
            print(f"ERROR: {e}")
            items = []
        
        if name == "buscar_productos":
            kw = args.get("keyword", "").lower()
            sk = args.get("sku", "").lower()
            lim = args.get("limite", 20)
            res = items
            if kw:
                res = [i for i in res if kw in i.get("name", "").lower() or kw in i.get("sku", "").lower()]
            if sk:
                res = [i for i in res if sk in i.get("sku", "").lower()]
            res = res[:lim]
            print(f"RESULTADOS: {len(res)}")
        elif name == "consultar_stock":
            sk = args.get("sku", "").lower()
            res = [i for i in items if i.get("sku", "").lower() == sk]
            if not res:
                res = {"error": f"No se encontró: {sk}"}
        elif name == "productos_stock_bajo":
            lim = args.get("limite", 50)
            res = [i for i in items if i.get("stock_on_hand", 0) < 10][:lim]
        elif name == "resumen_inventario":
            res = {
                "total_productos": len(items),
                "total_stock": sum(i.get("stock_on_hand", 0) for i in items),
                "valor_total": round(sum(i.get("stock_on_hand", 0) * i.get("rate", 0) for i in items), 2)
            }
        else:
            res = {"error": f"Herramienta no encontrada: {name}"}
        
        r = {"jsonrpc": "2.0", "id": mid, "result": {
            "content": [{"type": "text", "text": json.dumps(res, indent=2, ensure_ascii=False)}]
        }}
    else:
        r = {"jsonrpc": "2.0", "id": mid, "error": {"code": -32601, "message": "Method not found"}}
    
    if sid in sessions:
        await sessions[sid].put(json.dumps(r))
    return {"status": "ok"}

@app.get("/")
def root():
    return {"status": "ok", "total_tools": len(TOOLS["tools"])}
