from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import re
import os

with open("rsa_key.p8", "rb") as key_file:
  p_key = serialization.load_pem_private_key(
    key_file.read(),
    password = [private_password].encode(),
    backend = default_backend()
    )

pkb = p_key.private_bytes(
  encoding = serialization.Encoding.PEM,
  format = serialization.PrivateFormat.PKCS8,
  encryption_algorithm = serialization.NoEncryption()
  )

pkb = pkb.decode("UTF-8")
pkb = re.sub("-*(BEGIN|END) PRIVATE KEY-*\n","",pkb).replace("\n","")

print(pkb)