import requests
import time
import os
from concurrent.futures import ThreadPoolExecutor

def handler(pd: "pipedream"):
    # --- CONFIGURAZIONE ---
    # Come coordinatore security, usa os.environ per le chiavi!

    # 1. LETTURA OFFSET
    ds = pd.inputs["data_store"]
    offset = ds.get("last_offset", 0) 
    
    # IMPOSTIAMO IL BATCH A 40
    batch_size = 40 

    # 2. DOWNLOAD PRODOTTI (Sostituisce lo step custom_request)
    url_get = f"https://comprabene.biz/api/products?display=[id,name]&output_format=JSON&limit={offset},{batch_size}&ws_key={PS_KEY}"
    response = requests.get(url_get, timeout=20)
    products = response.json().get("products", [])
    
    if not products:
        return {"msg": "Fine catalogo", "offset": offset}

    # 3. FUNZIONE PARALLELA
    def process_product(p):
        p_id, p_name = p["id"], p["name"]
        try:
            # Chiamata OpenAI
            res_ai = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_KEY}"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": f"Scrivi descrizione SEO HTML per: {p_name}"}]
                },
                timeout=25
            )
            desc = res_ai.json()['choices'][0]['message']['content']

            # Update PrestaShop
            url_put = f"https://comprabene.biz/api/products/{p_id}?ws_key={PS_KEY}&method=PUT"
            xml = f"<prestashop><product><id>{p_id}</id><description><language id='1'><![CDATA[{desc}]]></language></description></product></prestashop>"
            requests.post(url_put, data=xml.encode('utf-8'), headers={'Content-Type': 'application/xml'}, timeout=20)
            
            return {"id": p_id, "status": "success"}
        except Exception as e:
            return {"id": p_id, "status": "error", "msg": str(e)}

    # 4. ESECUZIONE TURBO (10 thread contemporanei)
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = list(executor.map(process_product, products))

    # 5. AGGIORNAMENTO OFFSET
    new_offset = offset + len(products)
    ds.set("last_offset", new_offset)

    return {
        "batch_elaborato": len(products),
        "nuovo_offset": new_offset,
        "risultati": results
    }
