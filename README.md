# kb-trends-slack-agent (B안)

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# edit .env
Start Postgres
bash
코드 복사
docker-compose up -d
Run hourly (snapshot + delta alert)
bash
코드 복사
python -m app.main hourly
Run daily (rollup)
bash
코드 복사
python -m app.main daily
# or specific day
python -m app.main daily --date 2025-12-20
Scheduling (cron example)
hourly: every hour

daily: 23:55 KST

bash
코드 복사
0 * * * *  cd ~/kb-trends-slack-agent && . .venv/bin/activate && python -m app.main hourly >> logs/hourly.log 2>&1
55 23 * * * cd ~/kb-trends-slack-agent && . .venv/bin/activate && python -m app.main daily >> logs/daily.log 2>&1
yaml
코드 복사

---

## 실행 체크 (바로 확인)
1) DB 테이블 확인:
```bash
docker exec -it kb_trends_pg psql -U kb -d kbtrends -c "\dt"
hourly 한번 실행:

bash
코드 복사
python -m app.main hourly
daily 실행:

bash
코드 복사
python -m app.main daily
