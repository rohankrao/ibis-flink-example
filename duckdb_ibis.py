import ibis
import dill as pickle

@ibis.udf.scalar.python()
def add_one(x: int) -> int:
    return x + 1

if __name__ == '__main__':
    t = ibis.examples.penguins.fetch()
    t2 = t.select(year=add_one(t.year))
    b = pickle.dumps(t)
    t3 = pickle.loads(b)
    t3.execute()
