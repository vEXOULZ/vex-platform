"""Phase 0 spike: can procrastinate do what the shared layer needs?"""
import asyncio, sys, time
import psycopg, procrastinate
from procrastinate import RetryStrategy

DSN = "postgresql://vex:vex@127.0.0.1:55434/vex_platform_test"
SCHEMA = "procrastinate"
results = {}

app = procrastinate.App(connector=procrastinate.PsycopgConnector(
    conninfo=DSN, kwargs={"options": f"-c search_path={SCHEMA}"}))
seen = []

@app.task(name="slow", pass_context=True)
async def slow(context, n):
    try:
        await asyncio.sleep(30)
    except asyncio.CancelledError:
        seen.append(("cancelled", n)); raise

@app.task(name="flaky", retry=RetryStrategy(max_attempts=2, wait=0))
async def flaky(n):
    seen.append(("flaky", n)); raise RuntimeError("boom")

@app.task(name="locked")
async def locked(n):
    seen.append(("start", n, time.monotonic())); await asyncio.sleep(0.5); seen.append(("end", n, time.monotonic()))

async def main():
    with psycopg.connect(DSN, autocommit=True) as c:
        c.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE; CREATE SCHEMA {SCHEMA}")
        c.execute(f"SET search_path={SCHEMA}")
        c.execute(app.schema_manager.get_schema())
        n = c.execute("select count(*) from information_schema.tables where table_schema=%s", (SCHEMA,)).fetchone()[0]
        results["1_own_schema_tables"] = n
        results["1_public_clean"] = c.execute("select count(*) from information_schema.tables where table_schema='public' and table_name like 'procrastinate%'").fetchone()[0] == 0
    async with app.open_async():
        # 3: worker in the same loop as another long-running task ("uvicorn")
        ticks = []
        async def other():
            while True: ticks.append(1); await asyncio.sleep(0.1)
        other_t = asyncio.create_task(other())
        worker = asyncio.create_task(app.run_worker_async(concurrency=3, install_signal_handlers=False, abort_job_polling_interval=0.5, fetch_job_polling_interval=0.5))
        jid = await slow.defer_async(n=1)
        await asyncio.sleep(1)
        # 2: abort a running async job
        await app.job_manager.cancel_job_by_id_async(jid, abort=True)
        await asyncio.sleep(2)
        results["2_abort_cancelled_error"] = ("cancelled", 1) in seen
        results["2_status"] = str(await app.job_manager.get_job_status_async(jid))
        await flaky.defer_async(n=1); await asyncio.sleep(3)
        results["retry_attempts"] = sum(1 for s in seen if s[0] == "flaky")
        await locked.configure(lock="vod:1").defer_async(n=1)
        await locked.configure(lock="vod:1").defer_async(n=2)
        await asyncio.sleep(3)
        s = {x[1]: x[2] for x in seen if x[0] == "start"}; e = {x[1]: x[2] for x in seen if x[0] == "end"}
        results["lock_serialized"] = len(s) == 2 and (s[2] >= e[1] or s[1] >= e[2])
        # 4: restart worker with a different concurrency
        worker.cancel()
        try: await worker
        except asyncio.CancelledError: pass
        worker2 = asyncio.create_task(app.run_worker_async(concurrency=5, install_signal_handlers=False, fetch_job_polling_interval=0.5))
        await locked.defer_async(n=3); await asyncio.sleep(1.5)
        results["4_restart_worker_ok"] = any(x[:2] == ("end", 3) for x in seen)
        results["3_loop_shared_ticks"] = len(ticks) > 50
        worker2.cancel(); other_t.cancel()
        await asyncio.gather(worker2, other_t, return_exceptions=True)
    for k, v in sorted(results.items()): print(k, v)

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
asyncio.run(main())
