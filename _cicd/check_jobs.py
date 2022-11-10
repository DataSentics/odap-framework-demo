import yaml
import json
from databricks_cli.sdk import ApiClient, JobsService
import sys

with open("config.yaml", "r") as f:
    config = yaml.load(f, Loader=yaml.FullLoader)["parameters"]["segmentfactory"][
        "exports"
    ]

api_client = ApiClient(host=sys.argv[2], token=sys.argv[1])
jobs_service = JobsService(client=api_client)

ids = []
existing = []
for x in jobs_service.list_jobs()["jobs"]:
    if "tags" in x["settings"] and x["settings"]["tags"]["env"] == sys.argv[3]:
        ids.append(x["settings"]["tags"]["id"])
        existing.append(x)

for export in config:
    if export in ids:
        new_schedule = {
            "quartz_cron_expression": "0 0 0 1 * ?",
            "timezone_id": "UTC",
            "pause_status": "PAUSED",
        }
        if "schedule" in config[export]:
            new_schedule = config[export]["segments"]
        jobs_service.update_job(
            job_id=existing[ids.index(export)]["job_id"],
            new_settings={"schedule": new_schedule},
        )
    else:
        selected_seg = config[export]
        kwargs = {
            **{
                **{
                    "notebook_task": {
                        "notebook_path": "odap-framework-demo/_orchestration/job_orchestrator",
                        "base_parameters": {
                            "segment_names": json.dumps(list(selected_seg["segments"].keys())),
                            "export_name": config[export]["destination"],
                        },
                    }
                },
                **{"new_cluster": json.loads(sys.argv[4].replace("'", '"'))},
            },
            **{
                "name": f"Segment export {export}",
            },
            **{
                "tags": {
                    "env": sys.argv[3],
                    "id": export,
                },
                "max_concurrent_runs": 1,
                "schedule": selected_seg["schedule"]
                if "schedule" in selected_seg
                else None,
                "git_source": {
                    "git_url": "https://github.com/DataSentics/odap-framework-demo.git",
                    "git_provider": "gitHub",
                    "git_branch": "cicd_branch",
                },
            },
        }
        jobs_service.create_job(**kwargs)
