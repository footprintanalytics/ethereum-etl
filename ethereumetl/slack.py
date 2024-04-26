import requests


def send_to_slack(message):
    url = 'https://hooks.slack.com/services/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'
    text = f'【ethereum-etl】{message}'
    # requests.post(url, json={'text': text})
