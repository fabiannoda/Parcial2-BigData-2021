def handler(event, context):
    print("Hello from zappa")
    print(event)
    return {'status': 200}

def trigger(event, context):
    print("Hello from zappa in one trigger")
    print(event)
    return {'status': 200}
