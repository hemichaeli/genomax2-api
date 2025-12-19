import hashlib, hmac, json, os 
 
def canonicalize_and_hash(obj): 
    canonical = json.dumps(obj, sort_keys=True, separators=(',', ':')) 
    return hashlib.sha256(canonical.encode()).hexdigest() 
