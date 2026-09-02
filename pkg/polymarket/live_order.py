import json
import sys
from decimal import Decimal, ROUND_DOWN

from py_clob_client_v2 import (
    ApiCreds,
    ClobClient,
    OrderArgs,
    OrderType,
    PartialCreateOrderOptions,
    Side,
    SignatureTypeV2,
)


def resolve_order_type(order_type_text):
    requested = str(order_type_text or "IOC").upper()

    order_type = getattr(OrderType, requested, None)
    if order_type is not None:
        return order_type, requested

    if requested == "IOC":
        order_type = getattr(OrderType, "FAK", None)
        if order_type is not None:
            return order_type, "FAK"

    order_type = getattr(OrderType, "GTC", None)
    if order_type is None:
        raise RuntimeError(f"unsupported order type: {requested}")

    return order_type, "GTC"


def decimal_text(value, places):
    step = Decimal("1").scaleb(-places)
    number = Decimal(str(value)).quantize(step, rounding=ROUND_DOWN)
    text = format(number.normalize(), "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text or "0"


def is_invalid_amounts(error):
    return "invalid amounts" in str(error).lower()


def create_and_post_order(
    client,
    token_id,
    side_text,
    price_text,
    size_text,
    tick_size,
    neg_risk,
    order_type,
):
    signed_order = client.create_order(
        order_args=OrderArgs(
            token_id=token_id,
            price=float(price_text),
            size=float(size_text),
            side=Side.BUY if side_text == "BUY" else Side.SELL,
        ),
        options=PartialCreateOrderOptions(tick_size=tick_size, neg_risk=neg_risk),
    )
    return client.post_order(signed_order, order_type)


def size_candidates(size_text, price_text):
    original_size = Decimal(size_text)
    price = Decimal(price_text)
    original_notional = original_size * price

    seen = set()
    for places in (4, 3, 2, 1, 0):
        candidate = Decimal(decimal_text(original_size, places))
        if candidate <= 0:
            continue
        if candidate * price > original_notional:
            continue

        text = decimal_text(candidate, places)
        if text in seen:
            continue
        seen.add(text)
        yield text


def main():
    req = json.load(sys.stdin)

    host = req["host"]
    chain_id = int(req.get("chain_id") or 137)
    private_key = req["private_key"]
    funder_address = req["funder_address"]
    api_key = str(req.get("api_key") or "")
    api_secret = str(req.get("api_secret") or "")
    api_passphrase = str(req.get("api_passphrase") or "")
    token_id = str(req["token_id"])
    side_text = str(req["side"]).upper()
    price_text = decimal_text(req["price"], 6)
    size_text = decimal_text(req["size"], 4)
    price = Decimal(price_text)
    size = Decimal(size_text)
    order_type_text = str(req.get("order_type") or "IOC").upper()

    if side_text not in ("BUY", "SELL"):
        raise RuntimeError(f"unsupported side: {side_text}")
    if price <= 0:
        raise RuntimeError(f"invalid price: {price}")
    if size <= 0:
        raise RuntimeError(f"invalid size: {size}")

    if api_key and api_secret and api_passphrase:
        creds = ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        )
    else:
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

    order_type, effective_order_type = resolve_order_type(order_type_text)

    last_error = None
    submitted_size_text = size_text
    amount_retry_count = 0
    for candidate_size_text in size_candidates(size_text, price_text):
        try:
            resp = create_and_post_order(
                client,
                token_id,
                side_text,
                price_text,
                candidate_size_text,
                tick_size,
                neg_risk,
                order_type,
            )
            submitted_size_text = candidate_size_text
            break
        except Exception as exc:
            last_error = exc
            if not is_invalid_amounts(exc):
                raise
            amount_retry_count += 1
    else:
        raise last_error

    if isinstance(resp, dict):
        resp["requestedOrderType"] = order_type_text
        resp["effectiveOrderType"] = effective_order_type
        resp["submittedPrice"] = price_text
        resp["submittedSize"] = submitted_size_text
        resp["amountRetryCount"] = amount_retry_count
    else:
        resp = {
            "response": resp,
            "requestedOrderType": order_type_text,
            "effectiveOrderType": effective_order_type,
            "submittedPrice": price_text,
            "submittedSize": submitted_size_text,
            "amountRetryCount": amount_retry_count,
        }

    print(json.dumps(resp, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
