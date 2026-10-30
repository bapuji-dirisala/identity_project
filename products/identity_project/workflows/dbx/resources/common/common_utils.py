"""
Brickflow helper used by every `wf_*.py` workflow definition.

Centralises:
    - env resolution (local/dev/qa/prod),
    - env-prefixed workflow names (dev_wf_* vs prod_wf_*),
    - cluster builder with instance profile + policy,
    - YAML config loader (common + per-job).
"""

import ast
import os
import sys
import yaml

from brickflow import ctx, Cluster, NotebookTask


class CommonUtils:
    def deploy_env(self):
        if ctx.env == "local":
            return "dev"
        if ctx.env == "preprod":
            return "qa"
        return ctx.env

    def workflow_name(self, conf, base_name: str) -> str:
        """wf_identity_raw -> dev_wf_identity_raw (env prefix added by Brickflow deploy)."""
        suffix = base_name.removeprefix("wf_")
        return conf["prefix"] + suffix

    def create_job_cluster(self, job_args):
        env = self.deploy_env()
        aws_attributes = {
            "first_on_demand": 1,
            "availability": "ON_DEMAND",
            "instance_profile_arn": "arn:aws:iam::<CLOUD_ACCOUNT_ID>:instance-profile/sole/group/data",
            "spot_bid_price_percent": 100,
            "ebs_volume_count": 0,
            **job_args.get("aws_attributes", {}),
        }
        return Cluster(
            name=job_args.get("name"),
            spark_version=job_args.get("spark_version"),
            node_type_id=job_args.get("node_type_id"),
            driver_node_type_id=job_args.get("driver_node_type_id"),
            min_workers=job_args.get("min_workers"),
            max_workers=job_args.get("max_workers"),
            enable_elastic_disk=True,
            policy_id=job_args.get("policy_id"),
            aws_attributes=aws_attributes,
            custom_tags={**job_args.get("custom_tags", {}), "nike-environment": env},
        )

    def read_yaml(self, file_path):
        with open(file_path) as f:
            return yaml.safe_load(f)

    def get_config(self, cmn_prop_file_path: str, job_prop_file_path: str):
        root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        sys.path.append(root_dir)
        conf = self.read_yaml(f"{root_dir}/prop/{cmn_prop_file_path}.yaml")
        conf.update(self.read_yaml(f"{root_dir}/prop/{job_prop_file_path}.yaml"))
        return conf

    def create_notebook_task(
        self, notebook_path, base_parameters, catalog=None, schema=None
    ):
        if ctx.env == "local":
            notebook_path = f"{notebook_path}.py"
            source = "WORKSPACE"
        else:
            notebook_path = f"products/identity_project/{notebook_path}"
            source = "GIT"
        base_parameters = ast.literal_eval(base_parameters)
        return NotebookTask(
            notebook_path=notebook_path, base_parameters=base_parameters, source=source
        )
