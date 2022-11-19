import pika, json, psycopg2

conn = psycopg2.connect(
    host="localhost",
    database="TFG",
    user="postgres",
    password="Untitled#4"
)

# create a cursor


connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

def callback(ch, method, properties, body):
    consulta = json.loads(body.decode())

    print(consulta)

    #cur = conn.cursor()
    #cur.execute('select * from laudo limit 5')
    #print(cur.fetchall())

    #cur.close()

    #print(" [x] Received %r" % body.decode())
    ch.basic_ack(delivery_tag = method.delivery_tag)
    
channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue='task_queue', on_message_callback=callback)

channel.start_consuming()