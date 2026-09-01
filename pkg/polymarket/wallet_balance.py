import json
import sys

from py_clob_client_v2 import (
    ApiCreds,
    ClobClient,
    SignatureTypeV2,
)

try:
    from py_clob_client_v2 import AssetType, BalanceAllowanceParams
except ImportError:
    AssetType = None
    BalanceAllowanceParams = None


def main():
    req = json.load(sys.stdin)

    host = req["host"]
    chain_id = int(req.get("chain_id") or 137)
    private_key = req["private_key"]
    funder_address = req["funder_address"]
    api_key = str(req.get("api_key") or "")
    api_secret = str(req.get("api_secret") or "")
    api_passphrase = str(req.get("api_passphrase") or "")

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

    try:
        if AssetType is None or BalanceAllowanceParams is None:
            raise TypeError("BalanceAllowanceParams unavailable")
        resp = client.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        )
    except TypeError:
        resp = client.get_balance_allowance(asset_type="COLLATERAL")

    print(json.dumps(resp, ensure_ascii=False, default=str), flush=True)


if __name__ == "__main__":
    main()
