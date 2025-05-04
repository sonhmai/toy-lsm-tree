import random 

from lsm import LSMTree

if __name__ == "__main__":
    # Create database in the 'mydb' directory
    db = LSMTree("./mydb")

    # Store some user data
    db.set("user:1", {
        "name": "Alice",
        "email": "alice@example.com",
        "age": 30
    })

    # Read it back
    user = db.get("user:1")
    print(user['name'])  # Prints: Alice

    # Store many items
    for i in range(1000):
        db.set(f"item:{i}", {
            "name": f"Item {i}",
            "price": random.randint(1, 100)
        })

    # Range query example
    print("\nItems 10-15:")
    for key, value in db.range_query("item:10", "item:15"):
        print(f"{key}: {value}")