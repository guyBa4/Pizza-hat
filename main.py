import sys
import sqlite3


# DTO
class hats:
    def __init__(self, id, topping, supplier, quantity):
        self.id = id
        self.topping = topping
        self.supplier = supplier
        self.quantity = quantity


class suppliers:
    def __init__(self, id, name):
        self.id = id
        self.name = name


class orders:
    def __init__(self, id, location, hat):
        self.id = id
        self.location = location
        self.hat = hat


# DAO
class _Hats:
    def __init__(self, connection):
        self._connection = connection

    def insert(self, hat):
        with self._connection:
            c = self._connection.cursor()
            c.execute("""INSERT INTO hats (id, topping, supplier, quantity) VALUES (?,?,?,?)""",
                      [hat.id, hat.topping, hat.supplier, hat.quantity])

    def find(self, topping):
        with self._connection:
            c = self._connection.cursor()
            x = c.execute("""SELECT * FROM hats WHERE topping = ? ORDER BY supplier ASC""",
                          [topping]).fetchone()
            return hats(*x)

    def updateQuantity(self, topping):
        with self._connection:
            c = self._connection.cursor()
            thisHat = self.find(topping)
            if thisHat is not None:
                c.execute("""UPDATE hats SET quantity = ? WHERE id = ?""", [(thisHat.quantity - 1), thisHat.id])
                c.execute("""DELETE FROM hats WHERE quantity = 0""")
                self._connection.commit()
        return thisHat.supplier


class _Suppliers:
    def __init__(self, connection):
        self._connection = connection

    def insert(self, supplier):
        with self._connection:
            c = self._connection.cursor()
            c.execute("""INSERT INTO suppliers (id, name) VALUES (?,?)""",
                      [supplier.id, supplier.name])

    def find(self, ID):
        with self._connection:
            c = self._connection.cursor()
            x = c.execute("""SELECT * FROM suppliers WHERE id = ?""", [ID]).fetchone()
            return suppliers(*x)


class _Orders:
    def __init__(self, connection):
        self._connection = connection

    def insert(self, order):
        with self._connection:
            c = self._connection.cursor()
            c.execute("""INSERT INTO orders (id, location, hat) VALUES (?,?,?)""",
                      [order.id, order.location, order.hat])

    def find(self, orderId):
        with self._connection:
            c = self._connection.cursor()
            x = c.execute("""SELECT Name FROM orders WHERE id = ?""",
                          [orderId]).fetchone()
            return orders(*x)


# Repository

class Repository:

    def __init__(self, argv4):
        self._connection = sqlite3.connect(argv4)
        self.hats = _Hats(self._connection)
        self.suppliers = _Suppliers(self._connection)
        self.orders = _Orders(self._connection)
        self.create_tables()

    def _close(self):
        self._connection.commit()
        self._connection.close()

    def create_tables(self):
        with self._connection:
            c = self._connection.cursor()
            c.executescript("""
            CREATE TABLE IF NOT EXISTS hats (
                id INTEGER PRIMARY KEY,
                topping STRING NOT NULL,
                supplier INTEGER REFERENCES supplier(id),
                quantity INTEGER NOT NULL

            );
            CREATE TABLE IF NOT EXISTS suppliers (
                id INTEGER PRIMARY KEY,
                name STRING NOT NULL
            );
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY,
                location STRING NOT NULL,
                hat INTEGER REFERENCES hats(id)
            );
        """)

    def process_order(self, order_id, location, topping):
        x = self.hats.find(topping).id
        newOrder = orders(order_id, location, x)
        self.orders.insert(newOrder)
        supplier = self.hats.updateQuantity(topping)
        return self.suppliers.find(supplier).name

def main(argv):

    configfile = argv[1]
    ordersfile = argv[2]
    output = argv[3]
    db = argv[4]
    repo = Repository(db)
    with open(configfile, 'r') as config:
        Lines = config.readlines()
        first_line = Lines[0].rstrip()
        first_line = first_line.rstrip()
        first_line = first_line.split(',')

        hatsNum = int(first_line[0]) + 1
        linelen = len(Lines) - 1

        for i in range(hatsNum, linelen + 1):
            new_suppliers = Lines[i].rstrip().split(',')
            new = suppliers(*new_suppliers)
            repo.suppliers.insert(new)

        for i in range(1, hatsNum):
            new_hat = Lines[i].rstrip()
            new_hat = new_hat.split(',')
            newInsert = hats(*new_hat)
            repo.hats.insert(newInsert)


    orderId = 1
    with open(output, "w") as outputfile:
        with open(ordersfile, "r") as file:
            for line in file:
                order = line.strip()
                thisOrder = order.split(",")
                supplier = repo.process_order(orderId, thisOrder[0], thisOrder[1])
                orderId += 1
                outputfile.write(thisOrder[1] + "," + str(supplier) + "," + thisOrder[0] + '\n')

    repo._close()


if __name__ == '__main__':
    main(sys.argv)
