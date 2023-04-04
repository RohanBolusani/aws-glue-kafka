from flask import Flask, Response
from kafka import KafkaConsumer

app = Flask(__name__)


@app.route('/stream')
def stream():
    # Replace with the private IP address of the EC2 instance
    ec2_private_ip = 'localhost'

    # Create a Kafka consumer
    consumer = KafkaConsumer(
        'your_topic_name',
        bootstrap_servers=[f'{ec2_private_ip}:9092'],
        # Configure other consumer settings, if needed
    )

    def generate():
        # Process the messages from the Kafka source
        for message in consumer:
            # Yield the message value to the client
            yield message.value.decode() + '\n'

    return Response(generate(), mimetype='text/plain')


if __name__ == '__main__':
    app.run(debug=True)
