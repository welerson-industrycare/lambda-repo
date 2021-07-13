import json

print(event)

auth = 'Deny'

if event['authorizationToken'] == 'token'
    auth = 'Allow'
else:
    auth = 'Deny'


authResponse = {

    'principalId':'token',
    'policyDocument':{
        'Version': '2021-04-30',
        'Statement':[
            {
                'Action':'execute-api:Invoke',
                'Resource':[''],
                'Effect':auth
            }
        ]
    } 
}

return authResponse