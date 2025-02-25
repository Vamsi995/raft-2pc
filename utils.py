import pickle
import base64

# Serialize an object into a plain text
def object_to_txt(obj):
    message_bytes = pickle.dumps(obj)
    base64_bytes = base64.b64encode(message_bytes)
    txt = base64_bytes.decode('ascii')
    return txt

# De-serialize an object from a plain text
def txt_to_object(txt):
    base64_bytes = txt.encode('ascii')
    message_bytes = base64.b64decode(base64_bytes)
    obj = pickle.loads(message_bytes)
    return obj