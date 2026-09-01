import json
import sys

from py_clob_client_v2 import ClobClient, SignatureTypeV2


def cred_value(creds, *names):
    if isinstance(creds, dict):
        for name in names:
            value = creds.get(name)
            if value:
                return str(value)

    for name in names:
        value = getattr(creds, name, None)
        if value:
            return str(value)

    return ""


def main():
    req = json.load(sys.stdin)

    host = req["host"]
    chain_id = int(req.get("chain_id") or 137)
    private_key = req["private_key"]
    funder_address = req["funder_address"]

    client = ClobClient(
        host=host,
        chain_id=chain_id,
        key=private_key,
        signature_type=SignatureTypeV2.POLY_1271,
        funder=funder_address,
        use_server_time=True,
    )

    creds = client.create_or_derive_api_key()
    api_key = cred_value(creds, "api_key", "apiKey", "key")
    api_secret = cred_value(creds, "api_secret", "apiSecret", "secret")
    api_passphrase = cred_value(
        creds,
        "api_passphrase",
        "apiPassphrase",
        "passphrase",
    )

    if not api_key or not api_secret or not api_passphrase:
        raise RuntimeError(f"could not read api creds from SDK response: {creds}")

    print(
        json.dumps(
            {
                "api_key": api_key,
                "api_secret": api_secret,
                "api_passphrase": api_passphrase,
            },
            ensure_ascii=False,
        ),
        flush=True,
    )


if __name__ == "__main__":
    main()
