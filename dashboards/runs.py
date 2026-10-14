"""Job runs started from the app. A run executes as the job owner, so the app only needs CAN_MANAGE_RUN."""

import os
from dataclasses import dataclass

from databricks.sdk.service.jobs import BaseRun, RunLifeCycleState, RunResultState

from data import workspace

REFERENCE_JOB_ID = os.getenv("CS2_REFERENCE_JOB_ID")
ANALYSIS_JOB_ID = os.getenv("CS2_ANALYSIS_JOB_ID")

_ACTIVE = {
    RunLifeCycleState.BLOCKED,
    RunLifeCycleState.PENDING,
    RunLifeCycleState.QUEUED,
    RunLifeCycleState.RUNNING,
    RunLifeCycleState.TERMINATING,
    RunLifeCycleState.WAITING_FOR_RETRY,
}


@dataclass(frozen=True)
class RunStatus:
    run_id: int
    state: str
    active: bool
    succeeded: bool
    url: str | None


def _status(run: BaseRun) -> RunStatus:
    life_cycle = run.state.life_cycle_state if run.state else None
    result = run.state.result_state if run.state else None
    state = result or life_cycle
    return RunStatus(
        run_id=run.run_id,
        state=state.value if state else "UNKNOWN",
        active=life_cycle in _ACTIVE,
        succeeded=result == RunResultState.SUCCESS,
        url=run.run_page_url,
    )


def start(job_id: str) -> int:
    return workspace().jobs.run_now(job_id=int(job_id)).run_id


def latest(job_id: str | None) -> RunStatus | None:
    if not job_id:
        return None
    run = next(iter(workspace().jobs.list_runs(job_id=int(job_id), limit=1)), None)
    return None if run is None else _status(run)
