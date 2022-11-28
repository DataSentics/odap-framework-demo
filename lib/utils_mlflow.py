from mlflow.tracking.artifact_utils import _download_artifact_from_uri
from mlflow.store.artifact.runs_artifact_repo import RunsArtifactRepository
import json
import tempfile
import os
    
def get_artifact_json_from_mlflow(logged_model_path, source_importances=False):
    if source_importances:
        source_string = "/feature_importances.json"
    else:
        source_string = "/coefficients.json"
    artifact_uri = "/".join(RunsArtifactRepository.get_underlying_uri(logged_model_path).split("/")[:-1]) + source_string
    print(artifact_uri)
    """
    Imports a json artifact from a logged MLFlow experiment as a python dictionary.
    :param artifact_uri: a URI with a path to the artifact
        can be found in the MLFlow experiment in DBX when clicking at the desired
        artifact under "Full Path"
     :return artifact_dict: dictionary with the content of the json artifact
    EXAMPLE:
    artifact_uri = "dbfs:/databricks/mlflow-tracking/271385/1b6cwefef4/artifacts/coefficients.json"
    get_artifact_json_from_mlflow(artifact_uri)
    """
    
    filename = artifact_uri.split("/")[-1]

    with tempfile.TemporaryDirectory(dir="/local_disk0/tmp",
                                     prefix="artifact-json") as tmpdir:
        _download_artifact_from_uri(artifact_uri, tmpdir)
        with open(os.path.join(tmpdir, filename)) as f:
            artifact_dict = json.load(f)

    return artifact_dict
