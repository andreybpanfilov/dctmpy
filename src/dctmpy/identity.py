import socket

from dctmpy.crypto import build_dfc_identity


class Identity():
    attributes = ['trusted', 'hostname', 'keystore', 'keystore_password', 'private_key_password']

    def __init__(self, **kwargs):
        for attribute in Identity.attributes:
            setattr(self, attribute, kwargs.pop(attribute, None))
        if self.keystore_password is None:
            self.keystore_password = "dfc"
        if self.private_key_password is None:
            self.private_key_password = "!!dfc!!"
        if self.hostname is None:
            self.hostname = socket.gethostname()
        if self.trusted is None:
            self.trusted = False

    def get_auth_data(self):
        return build_dfc_identity(self.keystore, "dfc",
                                  self.keystore_password, self.private_key_password,
                                  self.hostname)
