import requests


def grant_tokens(name='', email='', amount=1):
    url = "https://api.koos.io/programs/programId/grant-tokens"
    payload = {
        "recipient": {
            "email": email,
            "name": name
        },
        "reason": "Test",
        "amount": 1
    }
    headers = {
        "Content-Type": "application/json",
        "x-api-key": ""
    }

    response = requests.request("POST", url, json=payload, headers=headers)

    print(response.text)