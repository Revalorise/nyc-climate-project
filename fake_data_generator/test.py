from faker import Faker

faker = Faker()

company_industry = faker.industry()
print(f"Company Industry: {company_industry}")