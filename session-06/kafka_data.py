"""Fake data for the Session 6 Kafka lab.

get_registered_user()  – the original generator from the old lab (one fake user, no key)
KeyedEvents            – events WITH a key and a per-key sequence number, for the
                         partition / ordering / consumer-group experiments

Try it:  python kafka_data.py
"""
import itertools
import random
import uuid
from datetime import datetime, timezone

from faker import Faker

fake = Faker()

# The nine Austrian federal states are our message KEYS: a small, fixed set,
# so you can see that the same key always lands in the same partition.
STATES = ["Burgenland", "Kaernten", "Niederoesterreich", "Oberoesterreich", "Salzburg",
          "Steiermark", "Tirol", "Vorarlberg", "Wien"]

ACTIONS = ["register", "login", "view", "add_to_cart", "purchase"]


def get_registered_user():
    """One fake registered user (original lab function, unchanged output)."""
    return {
        "name": fake.name(),
        "address": fake.address(),
        "created_at": fake.year()
    }


class KeyedEvents:
    """Produces (key, value) pairs.

    key   = a state name, e.g. "Tirol"
    value = {"run": ..., "state": ..., "seq": n, "action": ..., "name": ..., "ts": ...}

    seq counts 1, 2, 3, ... separately for every key. A consumer can therefore check:
    "for key Tirol, did I receive seq 1, 2, 3, ... in this order?"  ->  ordering per key.
    run is a short random id per generator, so a restarted producer does not look
    like "out of order" (its seq starts again at 1, but with a new run id).
    """

    def __init__(self, keys=STATES, seed=None):
        self.keys = list(keys)
        self.run = uuid.uuid4().hex[:6]
        self.seq = {k: itertools.count(1) for k in self.keys}
        self.rng = random.Random(seed)

    def next(self, key=None):
        key = key or self.rng.choice(self.keys)
        value = {
            "run": self.run,
            "state": key,
            "seq": next(self.seq[key]),
            "action": self.rng.choice(ACTIONS),
            "name": fake.name(),
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        }
        return key, value


if __name__ == "__main__":
    print(get_registered_user())
    gen = KeyedEvents()
    for _ in range(5):
        print(gen.next())
