import requests
import time
from datetime import datetime, timedelta, timezone
import re
from supabase import create_client, Client

# === CONFIGURATION ===
SUPABASE_URL = "YOUR_SUPABASE_URL"
SUPABASE_KEY = "YOUR_SUPABASE_KEY"
DISCORD_TOKEN = "YOUR_DISCORD_TOKEN"
SOCIALDATA_API_KEY = "YOUR_SOCIALDATA_API_KEY"

# Ветки Discord
THREAD_IDS = ["123...", "456..."]  # Список всех веток для мониторинга
CONTENT_THREAD_ID = "123..."      # Ветка с твитами (контентом)
XP_BOT_THREAD_ID = "789..."       # Ветка, где бот пишет XP

# Настройки времени
DISCORD_TARGET = datetime.now(timezone.utc) - timedelta(days=2)
CONTENT_TARGET = datetime.now(timezone.utc) - timedelta(days=30)

# === ОБНОВЛЕННЫЕ HEADERS (Эмуляция браузера) ===
HEADERS = {
    'Authorization': DISCORD_TOKEN,
    'Content-Type': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9,ru;q=0.8',
    'Origin': 'https://discord.com',
    'Referer': 'https://discord.com/channels/@me',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'X-Debug-Options': 'bugReporterEnabled',
    'Process-Primary': 'true'
}

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_discord_messages(thread_id, is_content=False):
    target_date = CONTENT_TARGET if is_content else DISCORD_TARGET
    messages = []
    last_id = None
    
    while True:
        url = f"https://discord.com/api/v9/channels/{thread_id}/messages?limit=100"
        if last_id:
            url += f"&before={last_id}"
        
        response = requests.get(url, headers=HEADERS)
        if response.status_code != 200:
            print(f"Error {response.status_code} on {thread_id}")
            break
            
        batch = response.json()
        if not batch:
            break
            
        for msg in batch:
            msg_date = datetime.fromisoformat(msg['timestamp'])
            if msg_date < target_date:
                return messages
            messages.append(msg)
            
        last_id = batch[-1]['id']
        time.sleep(1) # Пауза для имитации человеческого чтения
    return messages

def get_discord_member_info(user_id):
    # Пытаемся получить инфо о ролях и дате вступления
    # Чтобы не спамить API, можно сначала проверять в локальном кэше или старой базе
    url = f"https://discord.com/api/v9/users/{user_id}"
    try:
        # Здесь логика получения инфо через профиль или участников сервера
        # Для упрощения возвращаем структуру, если запрос прошел успешно
        res = requests.get(url, headers=HEADERS)
        if res.status_code == 200:
            return res.json()
    except:
        return None
    return None

def parse_xp_from_bot(text):
    # Регулярка для поиска XP в сообщениях бота
    match = re.search(r'(\d[\d\s,.]*)\s*XP', text)
    if match:
        clean_val = match.group(1).replace(' ', '').replace(',', '').replace('.', '')
        return int(clean_val)
    return None

def process_leaderboard():
    all_users = {}
    now = datetime.now(timezone.utc).isoformat()
    
    # Сначала берем старые данные для кэша ролей
    old_data = supabase.table("leaderboard_stats").select("*").execute()
    old_db_data = {item['user_id']: item for item in old_data.data} if old_data.data else {}

    for t_id in THREAD_IDS:
        is_content = (t_id == CONTENT_THREAD_ID)
        msgs = get_discord_messages(t_id, is_content)
        
        for m in msgs:
            u_id = m['author']['id']
            if u_id not in all_users:
                # Берем роли из старой базы, чтобы не запрашивать Discord заново
                existing = old_db_data.get(u_id, {})
                all_users[u_id] = {
                    'user_id': u_id,
                    'username': m['author']['username'],
                    'discord_messages': 0,
                    'twitter_posts': 0,
                    'total_score': 0,
                    'discord_roles': existing.get('discord_roles', []),
                    'discord_joined_at': existing.get('discord_joined_at'),
                    'updated_at': now
                }
            
            if is_content:
                # Логика поиска ссылок на твиты и проверки через tweet_cache
                if "twitter.com" in m['content'] or "x.com" in m['content']:
                    all_users[u_id]['twitter_posts'] += 1
            else:
                all_users[u_id]['discord_messages'] += 1

    # Парсинг XP из ветки бота
    bot_msgs = get_discord_messages(XP_BOT_THREAD_ID, False)
    for bm in bot_msgs:
        target_uid = bm.get('mentions', [{}])[0].get('id')
        if target_uid and target_uid in all_users:
            val = parse_xp_from_bot(bm['content'])
            if val:
                all_users[target_uid]['total_score'] = val

    # Upsert в базу данных
    user_list = list(all_users.values())
    for i in range(0, len(user_list), 50):
        batch = user_list[i:i+50]
        supabase.table("leaderboard_stats").upsert(batch).execute()
        print(f"Synced batch {i//50 + 1}")

if __name__ == "__main__":
    process_leaderboard()
