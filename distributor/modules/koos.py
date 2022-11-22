import requests
import logging

from distributor.config import KOOS_KEY, DEBUG


def grant_tokens(name, email, amount, reason):
    url = "https://api.koos.io/programs/1605f511-4c01-4403-bd7c-1de2cf9e6a24/grant-tokens"
    payload = {
        "recipient": {
            "email": email,
            "name": name
        },
        "reason": reason,
        "amount": amount
    }
    headers = {
        "Content-Type": "application/json",
        "x-api-key": KOOS_KEY
    }

    if not DEBUG:
        response = requests.request("POST", url, json=payload, headers=headers)
        logging.info(response.text)
        logging.info(response.status_code)
    else:
        logging.info("DEBUG=True; Tokens not sent")
