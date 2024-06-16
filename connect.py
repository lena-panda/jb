from clickhouse_driver import Client
import settings

client = Client(host=settings.CH_HOST,
                user=settings.CH_USER,
                password=settings.CH_PASSWORD,
                port=settings.CH_PORT,
                secure=settings.CH_SECURE,
                verify=settings.CH_VERIFY,
                ca_certs=settings.CH_CA_CERTS
                )
