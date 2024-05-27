import csv
from faker import Faker

faker = Faker()


def generate_customer_data(num_rows):
    with open('../data/customer_data.csv', 'w', newline='') as csvfile:
        fieldnames = ['first_name', 'last_name', 'address', 'email', 'username', 'password']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # write the header row
        writer.writeheader()

        # generate and write the stream_data rows
        for _ in range(num_rows):
            writer.writerow({
                'first_name': faker.first_name(),
                'last_name': faker.last_name(),
                'address': faker.address(),
                'email': faker.email(),
                'username': faker.user_name(),
                'password': faker.password()
            })

    print(f"Generated {num_rows} rows of stream_data and saved to 'customer_data.csv'.")


def generate_product_data(num_rows):
    with open('../data/product_data.csv', 'w', newline='') as csvfile:
        fieldnames = ['product_id', 'product_name', 'description', 'price', 'category']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for _ in range(num_rows):
            writer.writerow({
                'product_id': faker.uuid4(),
                'product_name': faker.catch_phrase(),
                'description': faker.text(max_nb_chars=100),
                'price': faker.pydecimal(left_digits=3, right_digits=2, positive=True),
                'category': faker.word()
            })

    print(f"Generated {num_rows} rows of product data and saved to 'product_data.csv'.")


def generate_order_data(num_rows):
    with open('../data/order_data.csv', 'w', newline='') as csvfile:
        fieldnames = ['order_id', 'customer_id', 'product_id', 'quantity', 'order_date', 'total_amount']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for _ in range(num_rows):
            writer.writerow({
                'order_id': faker.uuid4(),
                'customer_id': faker.uuid4(),
                'product_id': faker.uuid4(),
                'quantity': faker.random_int(min=1, max=10),
                'order_date': faker.date_between(start_date='-1y', end_date='today'),
                'total_amount': faker.pydecimal(left_digits=5, right_digits=2, positive=True)
            })

    print(f"Generated {num_rows} rows of order data and saved to 'order_data.csv'.")


if __name__ == '__main__':
    generate_customer_data(1000)
    generate_product_data(1000)
    generate_order_data(1000)
