"""Data-path helpers on the client: per-identity S3 and VAST DB clients. Keys are read, never printed.

Also registered as the module `rle`, so the genomics kit's handlers (vendor/genomics/functions/transform/main.py)
run unchanged against this demo's endpoints: they read rle.MNT/BUCKET/SCHEMA/REF and call rle.db().
"""
import json, os, sys
import urllib3
urllib3.disable_warnings()
from env import E, S3_EP, DB_EP, KAFKA, BUCKET, SCHEMA_G, MNT, KEYDIR, STATE

SCHEMA = SCHEMA_G
REF = "incoming/genomics/baseline/reference/GRCh38_full_analysis_set_plus_decoy_hla.fa"
MNT_DATA = f"{MNT}/data"
MNT_HOME = f"{MNT}/home"
MNT_DATASETS = f"{MNT}/datasets"


def key(user):
    user = {"rle-svc": f"{E['RLU_PREFIX']}-svc", "rle-analyst": f"{E['RLU_PREFIX']}-analyst"}.get(user, user)  # kit names
    p = os.path.join(KEYDIR, f"{user}.json")
    if os.access(p, os.R_OK):
        return json.load(open(p))[user]
    return json.load(open(os.path.join(STATE, "keys.json")))[user]


def s3(user):
    import boto3, botocore
    k = key(user)
    return boto3.client("s3", endpoint_url=S3_EP, aws_access_key_id=k["access_key"],
                        aws_secret_access_key=k["secret_key"], verify=False,
                        config=botocore.config.Config(retries={"max_attempts": 2},
                                                      request_checksum_calculation="when_required",
                                                      response_checksum_validation="when_required",
                                                      max_pool_connections=64))


def db(user):
    import vastdb
    k = key(user)
    return vastdb.connect(endpoint=DB_EP, access=k["access_key"], secret=k["secret_key"])


def err(e):
    import botocore
    if isinstance(e, botocore.exceptions.ClientError):
        return f"{e.response['ResponseMetadata']['HTTPStatusCode']} {e.response['Error'].get('Code')}"
    return f"{type(e).__name__}: {str(e)[:120]}"


# the kit's handlers import `rle`; give them this module, with the data view as their mount
MNT = MNT_DATA
sys.modules.setdefault("rle", sys.modules[__name__])
