<context>
The market_data_intraday_update DAG is loaded and paused. We need to unpause it and trigger a manual test run to verify it works end-to-end.

Note: The DAG has a market hours check that may skip execution if run outside NYSE hours (14:30-21:00 UTC / weekdays only). Current time should be evaluated.
</context>

<tasks>
1. **Check current UTC time** (to understand if market hours check will pass):
   date -u "+%A %H:%M UTC"

2. **Unpause the DAG**:
   docker-compose exec airflow-scheduler airflow dags unpause market_data_intraday_update

3. **Trigger a manual test run**:
   docker-compose exec airflow-scheduler airflow dags trigger market_data_intraday_update

4. **Wait and check run status** (wait 20 seconds for tasks to start):
   sleep 20 && docker-compose exec airflow-scheduler airflow dags list-runs -d market_data_intraday_update --limit 1

5. **Check task logs for the market hours check**:
   docker-compose exec airflow-scheduler airflow tasks logs market_data_intraday_update check_market_hours --limit 1 2>&1 | tail -30
</tasks>

<output_format>
## Test Execution Results

| Item | Value |
|------|-------|
| Current UTC time | [day HH:MM] |
| Within market hours | [yes/no] |
| DAG run state | [success/running/failed/skipped] |
| Market hours check result | [passed/skipped - reason] |

## Log Output
[Show relevant log lines from check_market_hours task]

## Next Steps
[What to do based on results]
</output_format>

Execute each task in order and provide the summary.