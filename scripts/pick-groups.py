#!/usr/bin/env python3
"""Подбирает id групп консюмеров так, чтобы координаторы групп оказались на заданных брокерах.

Координатор группы — это лидер партиции __consumer_offsets с номером
    Utils.abs(groupId.hashCode()) % offsets.topic.num.partitions
Использование: pick-groups.py <вывод kafka-topics --describe для __consumer_offsets> <брокер для группы 1> <брокер для группы 2>
Печатает:      APP_GROUPS_CONSUMER1=<id> APP_GROUPS_CONSUMER2=<id>
"""
import re
import sys


def java_hash(s):
    """Java String.hashCode() в виде знакового 32-битного числа."""
    h = 0
    for ch in s:
        h = (31 * h + ord(ch)) & 0xFFFFFFFF
    return h - (1 << 32) if h >= 1 << 31 else h


def kafka_abs(n):
    """org.apache.kafka.common.utils.Utils.abs: Math.abs, но 0 для Integer.MIN_VALUE."""
    return 0 if n == -(1 << 31) else abs(n)


# Шаг 1. Читаем аргументы и вывод describe, строим карту «номер партиции -> лидер».
describe, b1, b2 = open(sys.argv[1]).read(), int(sys.argv[2]), int(sys.argv[3])
leaders = {int(p): int(l) for p, l in re.findall(r"Partition: (\d+)\s+Leader: (-?\d+)", describe)}
if not leaders:
    sys.exit("в выводе describe нет партиций __consumer_offsets")
n = len(leaders)


def pick(base, broker):
    """Шаг 2. Перебирает id вида <base>-0, <base>-1, ... и возвращает первый, чей координатор на broker."""
    for i in range(10_000):
        gid = f"{base}-{i}"
        partition = kafka_abs(java_hash(gid)) % n
        if leaders[partition] == broker:
            print(f"# {gid}: __consumer_offsets-{partition}, лидер {broker}", file=sys.stderr)
            return gid
    sys.exit(f"не найден id группы для брокера {broker}")


# Шаг 3. Печатаем результат в виде переменных окружения для start-app.sh.
print(f"APP_GROUPS_CONSUMER1={pick('kafka-test-service-consumer-1', b1)} "
      f"APP_GROUPS_CONSUMER2={pick('kafka-test-service-consumer-2', b2)}")
