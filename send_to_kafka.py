import sys
import argparse
from confluent_kafka import Producer

parser = argparse.ArgumentParser()
parser.add_argument('--topic', required=True)
args = parser.parse_args()

# Kafka Producer 객체 생성
producer = Producer({'bootstrap.servers':'맥ip주소 추후 업로드 예정:9092'})  # Kafka 서버 연결 정보 설정
topic = args.topic  # 명령어 인수에서 받은 topic 값 저장

# 메시지 전송 후 호출될 콜백 함수 정의
def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        print(f"Sent frame of size {len(msg.topic())}")

while True:
    frame_bytes = sys.stdin.buffer.read(1024*64)
    if not frame_bytes:
        break
    producer.produce(topic, frame_bytes, callback=delivery_report) # 읽은 데이터 => kafka 전송 (콜백)
    producer.poll() # kafkaProducer 이벤트 처리

producer.flush() # 모든 데이터가 전송된 후 종료