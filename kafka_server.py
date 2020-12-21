import producer_server

def run_kafka_server():
    producer = producer_server.ProducerServer(
        bootstrap_servers='localhost:9092',
        topic='bipin.udacity.police.dept.service.calls',
        client_id= 'bipin.udacity.sf.crimes',
        input_file='../data/police-department-calls-for-service.json',
    )
    return producer

def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()