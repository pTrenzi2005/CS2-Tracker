import uvicorn
from fastapi import FastAPI, Request

app = FastAPI()

@app.post("/")
async def gsi_endpoint(request: Request):
    try:
        data = await request.json()
        
        if 'player' in data:
            player = data['player']
            
            state = player.get('state', {})
            hp = state.get('health', 'Brak')
            
            position = player.get('position', 'Brak (nie w grze)')
            
            stats = player.get('match_stats', {})
            kills = stats.get('kills', 0)
            deaths = stats.get('deaths', 0)
            
            
            #print(f"Player data: {player}\n")

        print(f"DATA: {data}")
            
    except Exception:
        pass

    return {"status": "success"}

if __name__ == '__main__':
    print("Serwer GSI oparty na FastAPI uruchomiony na porcie 3000...")
    uvicorn.run(app, host="127.0.0.1", port=3000, log_level="warning")