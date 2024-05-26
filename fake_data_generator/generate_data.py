import csv
from faker import Faker

faker = Faker()


def generate_customer_data(num_rows):
    with open('../batch_data/customer_details.csv', 'w', newline='') as csvfile:
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

    print(f"Generated {num_rows} rows of stream_data and saved to 'customer_details.csv'.")


def generate_credit_card_data(num_rows):
    with open('../stream_data/credit_card_details.csv', 'w', newline='') as csvfile:
        fieldnames = ['card_number', 'cc_expire_date', 'cvv', 'provider']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # write the header row
        writer.writeheader()

        # generate and write the stream_data rows
        for _ in range(num_rows):
            writer.writerow({
                'card_number': faker.credit_card_number(),
                'cc_expire_date': faker.credit_card_expire(),
                'cvv': faker.credit_card_security_code(),
                'provider': faker.credit_card_provider()
            })

    print(f"Generated {num_rows} rows of stream_data and saved to 'credit_card_details.csv'.")


def stream_credit_card_data():
    data = {
        'card_number': faker.credit_card_number(),
        'cc_expire_date': faker.credit_card_expire(),
        'cvv': faker.credit_card_security_code(),
        'provider': faker.credit_card_provider()
    }

    return data


def generate_company_data(num_rows):
    with open('../batch_data/company_details.csv', 'w', newline='') as csvfile:
        fieldnames = ['company_name', 'company_address', 'company_phone']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # write the header row
        writer.writeheader()

        # generate and write the stream_data rows
        for _ in range(num_rows):
            writer.writerow({
                'company_name': faker.company(),
                'company_address': faker.address(),
                'company_phone': faker.phone_number()
            })

    print(f"Generated {num_rows} rows of stream_data and saved to 'company_details.csv'.")


def generate_shop_activity_data(num_rows):
    with open('../stream_data/shop_activity.csv', 'w', newline='') as csvfile:
        fieldnames = ['timestamp', 'user_id', 'event_type', 'product_id', 'price']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for _ in range(num_rows):
            writer.writerow({
                'timestamp': faker.date_time_this_year().isoformat(),
                'user_id': faker.uuid4(),
                'event_type': faker.random_element(['purchase', 'view', 'add_to_cart']),
                'product_id': faker.uuid4(),
                'price': faker.pricetag()
            })

    print(f"Generated {num_rows} rows of stream_data and saved to 'shop_activity.csv'.")


def stream_shop_activity_data():
    data = {
        'timestamp': faker.date_time_this_year().isoformat(),
        'user_id': faker.uuid4(),
        'event_type': faker.random_element(['purchase', 'view', 'add_to_cart']),
        'product_id': faker.uuid4(),
        'price': faker.pricetag()
    }

    return data


if __name__ == '__main__':
    generate_customer_data(1)
    generate_credit_card_data(1)
    generate_company_data(1)
    generate_shop_activity_data(1)
