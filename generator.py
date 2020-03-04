#!/usr/bin/python3
from datetime import datetime, date, time, timedelta
from numpy.random import choice, randint
from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka.errors import KafkaError


def get_random_value():
        
    dt = datetime.today() - timedelta(110)
    BUSINESS_DT = datetime.today() - timedelta(140)
    print(dt)
    print(BUSINESS_DT)
    new_dict = {}
    branch_list = ['Kazan', 'SPB', 'Novosibirsk', 'Surgut','Moscow','Irkutsk']
    currency_list = ['RUB', 'USD', 'EUR', 'GBP','TKL','JPS']
    #zp_list = ['10000', '50000','100000', '200000','300','590',]
    #new_dict['CH_PRODUCT_ID'] = numGenForever()
    new_dict['branch'] = choice(branch_list)
    new_dict['currency'] = choice(currency_list)
    new_dict['total'] = randint(10.50, 100002379.50)#choice(zp_list)
    new_dict['amount'] = randint(-100, 100)
    new_dict['datatime'] = datetime.strftime(dt, '%Y-%m-%d-%H-%M-%S')  #datetime.strftime(datetime.now(), '%d-%m-%Y-%H-%M-%s')
    new_dict['BUSINESS_DT'] = datetime.strftime(BUSINESS_DT, '%Y-%m-%d-%H-%M-%S') 
    new_dict['new_field'] = randint(300, 999)
    print(new_dict)
    return new_dict
# data = get_random_value()
# print(data)
#-------------------------------------------------
# Begin
#-------------------------------------------------
if __name__ == "__main__":

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x:dumps(x).encode('utf-8'),
                             compression_type='gzip')
    my_topic = 'transaction'

    while True:  
        for _ in range(100):
            data = get_random_value()

            try:
                future = producer.send(topic = my_topic, value = data)
                record_metadata = future.get(timeout=1000)
                
                print('--> The message has been sent to a topic: {}, partition: {}, offset: {}' \
                        .format(record_metadata.topic, 
                            record_metadata.partition, 
                            record_metadata.offset ))   
                                         
            except Exception as e:
                print('--> It seems an Error occurred: {}'.format(e))

            finally:
                producer.flush()

        sleep(1)

    producer.close()
