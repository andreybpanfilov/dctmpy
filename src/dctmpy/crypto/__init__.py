import time

import jks
from OpenSSL import crypto


def build_dfc_identity(keystore, key_name, keystore_password, key_password, hostname):
    pk = get_private_key(keystore, key_name, keystore_password, key_password)
    if not pk:
        return None
    cert = pk.cert_chain[0][1]
    cert = crypto.load_certificate(crypto.FILETYPE_ASN1, cert)
    cn = cert.get_subject().commonName
    if pk.algorithm_oid == jks.util.RSA_ENCRYPTION_OID:
        pk = crypto.load_privatekey(crypto.FILETYPE_ASN1, pk.pkey)
    else:
        pk = crypto.load_privatekey(crypto.FILETYPE_ASN1, pk.pkey_pkcs8)
    data = "%s\t%d\t%s\t%s" % (cn, time.time(), hostname, "")
    signature = crypto.sign(pk, data, b"sha1")
    return str("%s\t%s" % (data, jks.base64.b64encode(signature)))


def get_private_key(keystore, key_name, keystore_password, key_password):
    ks = jks.KeyStore.loads(keystore, keystore_password)
    for alias, pk in ks.private_keys.items():
        if alias != key_name:
            continue
        if not pk.is_decrypted():
            pk.decrypt(key_password)
        return pk
