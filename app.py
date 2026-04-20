import os
import json
import uuid
import time
import boto3

expediente = "740652"
nombre_completo = "Jorge Antonio Flores Burgos"

AWS_REGION    = os.getenv("AWS_REGION", "us-east-1")
SQS_URL      = os.getenv("SQS_QUEUE_URL", "")
SNS_TOPIC    = os.getenv("SNS_TOPIC_ARN", "")
DDB_TABLE     = os.getenv("DDB_TABLE", "boletines")
MOSTRADOR_URL = os.getenv("MOSTRADOR_BASE_URL", "http://localhost:8002")

sqs   = boto3.client("sqs", region_name=AWS_REGION)
sns   = boto3.client("sns", region_name=AWS_REGION)
ddb   = boto3.resource("dynamodb", region_name=AWS_REGION)
tabla = ddb.Table(DDB_TABLE)


def procesar_mensaje(msg):
    body = json.loads(msg["Body"])
    boletin_id = str(uuid.uuid4())

    # 1) Guardar en DynamoDB
    tabla.put_item(Item={
        "boletinID": boletin_id,
        "contenido": body["contenido"],
        "correo": body["correo"],
        "imagen_url": body["imagen_url"],
        "leido": False,
    })

    # 2) Enviar notificación por SNS
    link = f"{MOSTRADOR_URL}/boletines/{boletin_id}?correoElectronico={body['correo']}"
    sns.publish(
        TopicArn=SNS_TOPIC,
        Subject="Nuevo boletín disponible",
        Message=f"Se ha generado un nuevo boletín.\n\nVer contenido: {link}",
    )

    print(f"[OK] Boletín {boletin_id} guardado y notificado")


def consumir():
    print("Receptor escuchando la cola...")
    while True:
        try:
            resp = sqs.receive_message(
                QueueUrl=SQS_URL,
                MaxNumberOfMessages=5,
                WaitTimeSeconds=20,  # long polling
            )
            for msg in resp.get("Messages", []):
                try:
                    procesar_mensaje(msg)
                    sqs.delete_message(
                        QueueUrl=SQS_URL,
                        ReceiptHandle=msg["ReceiptHandle"],
                    )
                except Exception as e:
                    print(f"[ERROR procesando] {e}")
        except Exception as e:
            print(f"[ERROR cola] {e}")
            time.sleep(5)


if __name__ == "__main__":
    print("Servicio receptor listo")
    print(f"Alumno: {nombre_completo} | Expediente: {expediente}")
    consumir()