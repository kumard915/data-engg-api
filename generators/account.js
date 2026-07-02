import { faker } from "@faker-js/faker";
import { nanoid } from "nanoid";
import { randomBank, randomZone } from "../utils/faker.js";

export function generateAccounts(existing = [], date) {
  const accounts = (
    Array.isArray(existing) ? existing.filter(Boolean) : []
  ).slice();

  if (accounts.length === 0) {
    for (let i = 0; i < 30; i++) {
      accounts.push({
        id: nanoid(12),
        bank: randomBank(),
        accName: faker.person.fullName(),
        accNum: faker.finance.accountNumber(10),
        ifsc: "IFSC" + faker.string.numeric(6),
        vpa: faker.internet.userName() + "@okbizaxis",
        zone: randomZone(),
        maxLimit: faker.number.int({ min: 100000, max: 1000000 }),
        avgTicketSize: faker.number.int({ min: 500, max: 5000 }),
        enabled: true,
        createdOn: date + "T08:00:00.000Z",
        updatedOn: date + "T08:00:00.000Z",
      });
    }
  } else {
    if (Math.random() < 0.1) {
      const addCount = faker.number.int({ min: 1, max: 3 });
      for (let i = 0; i < addCount; i++) {
        accounts.push({
          id: nanoid(12),
          bank: randomBank(),
          accName: faker.person.fullName(),
          accNum: faker.finance.accountNumber(10),
          ifsc: "IFSC" + faker.string.numeric(6),
          vpa: faker.internet.userName() + "@okbizaxis",
          zone: randomZone(),
          maxLimit: faker.number.int({ min: 100000, max: 1000000 }),
          avgTicketSize: faker.number.int({ min: 500, max: 5000 }),
          enabled: true,
          createdOn: date + "T08:00:00.000Z",
          updatedOn: date + "T08:00:00.000Z",
        });
      }
    }
  }

  accounts.forEach((a) => {
    if (a && Math.random() < 0.05) a.updatedOn = date + "T09:00:00.000Z";
  });

  return accounts;
}
