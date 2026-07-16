"""janitor_daily — the morning judgment over last night's ingest run.

02:00 ingest_daily fires the PlatformRuns. Boards get ONE attempt by doctrine,
so a run that can't finish never fails on its own — at 06:00 this DAG closes
the window: terminate whatever is still open, then hand judgment to a
time-boxed (1h) Claude agent on Sonnet that categorizes each platform's
failure, re-fires transients, and opens an MR for code bugs. It never merges
and never touches main — the loop converges over mornings, not in one night.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum

from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s

INGEST_IMAGE = "{{ var.value.ingest_image }}"
# ingest + claude CLI + git (Dockerfile.janitor). Separate image so the plain
# worker/trigger image never carries agent tooling.
JANITOR_IMAGE = "{{ var.value.ingest_janitor_image }}"
ENV_SECRET = "ingest-runtime"    # TEMPORAL_ADDRESS + CH_*
AGENT_SECRET = "ingest-janitor"  # CLAUDE_CODE_OAUTH_TOKEN + git push creds

_COMMON = dict(
    namespace="shared",
    get_logs=True,
    on_finish_action="delete_succeeded_pod",
    env_from=[k8s.V1EnvFromSource(
        secret_ref=k8s.V1SecretEnvSource(name=ENV_SECRET))],
)

with DAG(
    dag_id="janitor_daily",
    description="Close the nightly window; agent-triage failures; refire",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2026, 7, 16, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    tags=["ingest", "temporal", "janitor", "agent"],
) as dag:
    stop_stuck = KubernetesPodOperator(
        task_id="stop_stuck",
        name="ingest-janitor-stop",
        image=INGEST_IMAGE,
        cmds=["python", "-u", "-m", "ingest.orchestration.janitor", "stop"],
        env_vars=[k8s.V1EnvVar(name="TEMPORAL_NAMESPACE", value="ingest")],
        **_COMMON,
    )

    triage = KubernetesPodOperator(
        task_id="triage",
        name="ingest-janitor-triage",
        image=JANITOR_IMAGE,
        cmds=["python", "-u", "-m", "ingest.orchestration.janitor", "triage"],
        env_vars=[
            k8s.V1EnvVar(name="TEMPORAL_NAMESPACE", value="ingest"),
            k8s.V1EnvVar(name="RUN_DATE", value="{{ ds }}"),
            k8s.V1EnvVar(name="JANITOR_MODEL", value="sonnet"),
        ],
        # the one-hour box: the task is killed at the deadline, red, visible.
        execution_timeout=timedelta(hours=1),
        namespace="shared",
        get_logs=True,
        on_finish_action="delete_succeeded_pod",
        env_from=[
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name=ENV_SECRET)),
            k8s.V1EnvFromSource(
                secret_ref=k8s.V1SecretEnvSource(name=AGENT_SECRET)),
        ],
    )

    stop_stuck >> triage
