import { faker } from "@faker-js/faker";
import { nanoid } from "nanoid";

export function generateMerchants(existing = [], date) {
  const merchants = (Array.isArray(existing) ? existing.filter(Boolean) : []).slice();

  if (merchants.length === 0) {
    // initial seed
    for (let i = 0; i < 10; i++) {
      merchants.push({
        id: nanoid(20),
        name: faker.company.name(),
        alias: faker.helpers.slugify(faker.company.name()).toLowerCase(),
        category: faker.helpers.arrayElement(['E-COMMERCE','RETAIL','SERVICES','B2B']),
        contactName: faker.person.fullName(),
        contactEmail: faker.internet.email(),
        contactMobile: faker.phone.number('9#########'),
        limit: faker.number.int({ min: 100000, max: 1000000 }),
        activeSince: faker.date.past({ years: 5 }).toISOString().split('T')[0],
        kycStatus: faker.helpers.arrayElement(['PENDING','VERIFIED','REJECTED']),
        enabled: true,
        createdOn: date + 'T10:00:00.000Z',
        updatedOn: date + 'T10:00:00.000Z'
      });
    }
  } else {
    // occasional new
    if (Math.random() < 0.05) {
      const addCount = faker.number.int({ min: 1, max: 2 });
      for (let i = 0; i < addCount; i++) {
        merchants.push({
          id: nanoid(20),
          name: faker.company.name(),
          alias: faker.helpers.slugify(faker.company.name()).toLowerCase(),
          category: faker.helpers.arrayElement(['E-COMMERCE','RETAIL','SERVICES','B2B']),
          contactName: faker.person.fullName(),
          contactEmail: faker.internet.email(),
          contactMobile: faker.phone.number('9#########'),
          limit: faker.number.int({ min: 100000, max: 1000000 }),
          activeSince: faker.date.past({ years: 5 }).toISOString().split('T')[0],
          kycStatus: faker.helpers.arrayElement(['PENDING','VERIFIED','REJECTED']),
          enabled: true,
          createdOn: date + 'T10:00:00.000Z',
          updatedOn: date + 'T10:00:00.000Z'
        });
      }
    }
  }

  merchants.forEach(m => {
    if (m && Math.random() < 0.1) m.updatedOn = date + 'T12:00:00.000Z';
  });

  return merchants;
}
