from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
import sys

try:
    with open('/opt/airflow/keys/rsa_key.p8', 'rb') as f:
        encrypted_key = f.read()
    
    private_key = serialization.load_pem_private_key(
        encrypted_key,
        password=b'root',
        backend=default_backend()
    )
    
    # Write unencrypted key to temp
    unencrypted_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )
    
    with open('/tmp/rsa_key_unencrypted.p8', 'wb') as f:
        f.write(unencrypted_pem)
    
    print('Unencrypted key created at /tmp/rsa_key_unencrypted.p8')
except Exception as e:
    print(f'Error: {e}', file=sys.stderr)
    sys.exit(1)
