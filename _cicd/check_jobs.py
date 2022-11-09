import yaml
import json
from databricks_cli.sdk import ApiClient, JobsService

import sys
print(sys.argv)
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
    for segment in config[export]["segments"]:
        if segment in ids:
            new_schedule = {
                "quartz_cron_expression": "0 0 0 1 * ?",
                "timezone_id": "UTC",
                "pause_status": "PAUSED",
            }
            if "schedule" in config[export]["segments"][segment]:
                new_schedule = config[export]["segments"][segment]["schedule"]
            jobs_service.update_job(
                job_id=existing[ids.index(segment)]["job_id"],
                new_settings={"schedule": new_schedule},
            )
        else:
            selected_seg = config[export]["segments"][segment]
            kwargs = {
                **{
                    **{
                        "notebook_task": {
                            "notebook_path": "odap-framework-demo/_orchestration/job_orchestrator",
                            "base_parameters": {
                                "segment_name": segment,
                                "export_name": config[export]["destinations"][0],
                            },
                        }
                    },
                    **{"new_cluster": json.loads("".join(sys.argv[4:]))},
                },
                **{
                    "name": f"Segment export '{selected_seg['lit']['name']}'",
                },
                **{
                    "tags": {
                        "env": sys.argv[3],
                        "id": segment,
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
