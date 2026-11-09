"""Shared helpers for the data-path side (runs on client-n5). Keys come from keys.json (mode 600); never printed."""
import json, os, urllib3
urllib3.disable_warnings()
import boto3, botocore

HERE = os.path.dirname(os.path.abspath(__file__))
S3_EP = "https://198.18.0.43"          # rad-lab-enclave VIP pool (.43-.45)
DB_EP = "http://198.18.0.44"
KAFKA = "198.18.0.46:9092"
BUCKET = "rad-lab-enclave-cohort"
SCHEMA = "genomics"
MNT = "/mnt/rad-lab-enclave"               # NFSv3 mount of /rad-lab-enclave/cohort on n5
REF = "incoming/reference/GRCh38_full_analysis_set_plus_decoy_hla.fa"

KEYDIR = "/srv/rad-lab-enclave/keys"     # one mode-600 file per identity, owned by that identity's uid

def key(user):
    p = os.path.join(KEYDIR, f"{user}.json")
    if os.access(p, os.R_OK):
        return json.load(open(p))[user]
    return json.load(open(os.path.join(HERE, "keys.json")))[user]

def s3(user):
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
    if isinstance(e, botocore.exceptions.ClientError):
        return f"{e.response['ResponseMetadata']['HTTPStatusCode']} {e.response['Error'].get('Code')}"
    return f"{type(e).__name__}: {str(e)[:120]}"
