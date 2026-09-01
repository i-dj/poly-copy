import json
import sys

from py_clob_client_v2 import ClobClient, SignatureTypeV2


def main():
    req = json.load(sys.stdin)

    host = req["host"]
    chain_id = int(req.get("chain_id") or 137)
    private_key = req["private_key"]
    funder_address = req["funder_address"]
    order_id = str(req["order_id"])

    if not order_id:
        raise RuntimeError("missing order_id")

    bootstrap = ClobClient(
        host=host,
        chain_id=chain_id,
        key=private_key,
        signature_type=SignatureTypeV2.POLY_1271,
        funder=funder_address,
        use_server_time=True,
    )
    creds = bootstrap.create_or_derive_api_key()

    client = ClobClient(
        host=host,
        chain_id=chain_id,
        key=private_key,
        creds=creds,
        signature_type=SignatureTypeV2.POLY_1271,
        funder=funder_address,
        use_server_time=True,
    )

    try:
        resp = client.get_order(order_id)
    except TypeError:
        resp = client.get_order(order_id=order_id)

    print(json.dumps(resp, ensure_ascii=False, default=str), flush=True)


if __name__ == "__main__":
    main()
