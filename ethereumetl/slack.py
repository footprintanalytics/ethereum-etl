import requests

members_handle = {
    'Bingo': '<@U010Y124P6J>',
    'Alex': '<@U02F7MQ5A7L>',
    'Rowland': '<@U010L7XAU0H>',
    'Philip': '<@U010HE2124A>',
    'Duke': '<@U010W306PJB>',
    'Eric': '<@U010VJUBNH4>',
    'Rosen': '<@U02F4MDKXKP>',
    'Ryan': '<@U02HSQQDX71>',
    'Orange': '<@U010VQFJQ0L>',
    'Ceiling': '<@U010TJ3TL77>',
    'Acer': '<@U010VKYJTJT>',
    'Zoni': '<@U010Q6WMT09>',
}


def send_to_slack(message):
    url = 'https://hooks.slack.com/services/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'
    text = f'【ethereum-etl】```{message}```'
    requests.post(url, json={'text': text})
