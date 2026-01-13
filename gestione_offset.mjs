import requests
import time
import os  # Essenziale per la sicurezza

def handler(pd: "pipedream"):
    # --- CONFIGURAZIONE SICURA ---
    # Recuperiamo le chiavi dalle Environment Variables di Pipedream
    # NON scrivere mai le chiavi reali qui.
    PS_KEY = os.environ.get("PS_KEY")
    OPENAI_KEY = os.environ.get("OPENAI_KEY")

    if not PS_KEY or not OPENAI_KEY:
        return {"error": "Chiavi API mancanti nelle variabili d'ambiente!"}

    # 1. LETTURA CURSORE (Dal Data Store collegato)
    ds = pd.inputs["data_store"]
    offset = ds.get("last_offset", 0) 
    
    # 2. RECUPERO BATCH 
    batch_size = 15
    url_get = f"https://comprabene.biz/api/products?display=[id,name]&output_format=JSON&limit={offset},{batch_size}&ws_key={PS_KEY}"
    
    response = requests.get(url_get)
    products = response.json().get("products", [])
    
    if not products:
        return {"msg": "Catalogo terminato o nessun prodotto trovato", "offset_finale": offset}

    report = []

    # 3. CICLO DI ELABORAZIONE
    for p in products:
        p_id = p["id"]
        p_name = p["name"]
        
        try:
            # --- CHIAMATA OPENAI ---
            res_ai = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {OPENAI_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "gpt-4o-mini",
                    "messages": [{"role": "user", "content": f"Scrivi una descrizione SEO professionale e accattivante in HTML per il prodotto: {p_name}"}]
                },
                timeout=20
            )
            
            # Gestione errore risposta AI
            if res_ai.status_code != 200:
                raise Exception(f"OpenAI Error: {res_ai.text}")

            new_desc = res_ai.json()['choices'][0]['message']['content']

            # --- UPDATE PRESTASHOP (PUT via POST bypass) ---
            url_put = f"https://comprabene.biz/api/products/{p_id}?ws_key={PS_KEY}&method=PUT"
            xml_data = f"""<prestashop><product><id>{p_id}</id><description><language id="1"><![CDATA[{new_desc}]]></language></description></product></prestashop>"""
            
            r_put = requests.post(
                url_put, 
                data=xml_data.encode('utf-8'), 
                headers={'Content-Type': 'application/xml'}, 
                timeout=20
            )
            
            report.append({"id": p_id, "name": p_name, "status": r_put.status_code})
            
        except Exception as e:
            report.append({"id": p_id, "name": p_name, "status": "error", "error": str(e)})
        
        time.sleep(0.5) # Pausa di sicurezza

    # 4. COMMIT DEL CURSORE
    new_offset = offset + len(products)
    ds.set("last_offset", new_offset)

    return {
        "success": True,
        "stato_avanzamento": f"Processati prodotti da {offset} a {new_offset}",
        "batch": report
    }
