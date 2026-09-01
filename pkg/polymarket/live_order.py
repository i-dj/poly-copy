import json
import sys

from py_clob_client_v2 import (
    ClobClient,
    OrderArgs,
    OrderType,
    PartialCreateOrderOptions,
    Side,
    SignatureTypeV2,
)


def main():
    req = json.load(sys.stdin)

    host = req["host"]
    chain_id = int(req.get("chain_id") or 137)
    private_key = req["private_key"]
    funder_address = req["funder_address"]
    token_id = str(req["token_id"])
    side_text = str(req["side"]).upper()
    price = float(req["price"])
    size = float(req["size"])

    if side_text not in ("BUY", "SELL"):
        raise RuntimeError(f"unsupported side: {side_text}")
    if price <= 0:
        raise RuntimeError(f"invalid price: {price}")
    if size <= 0:
        raise RuntimeError(f"invalid size: {size}")

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

    tick_size = client.get_tick_size(token_id)
    neg_risk = client.get_neg_risk(token_id)

    signed_order = client.create_order(
        order_args=OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=Side.BUY if side_text == "BUY" else Side.SELL,
        ),
        options=PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk),
    )

    resp = client.post_order(signed_order, OrderType.GTC)
    print(json.dumps(resp, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
