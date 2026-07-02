import { faker } from "@faker-js/faker";

export const randomBank = () =>
  faker.helpers.arrayElement([
    "HDFC",
    "ICICI",
    "Axis Bank",
    "State Bank of India",
    "Paytm",
    "GPay",
  ]);

export const randomZone = () =>
  faker.helpers.arrayElement(["COMPANY_7", "COMPANY_77", "COMPANY_82"]);

export const randomStatus = () =>
  faker.helpers.arrayElement([
    "SUCCESS",
    "PENDING_PAYMENT",
    "FAILED",
    "SUCCESS_MANUAL",
  ]);

// export const randomAmount = (min = 100, max = 50000) =>
//   parseFloat(faker.finance.amount(min, max, 2));
export const randomAmount = (min = 100, max = 50000) => {
  return Math.round(
    Number(
      faker.finance.amount({
        min,
        max,
        dec: 2,
      })
    ) * 100
  );
};

export const randomDateTime = (date) =>
  faker.date
    .between(date + "T00:00:00.000Z", date + "T23:59:59.999Z")
    .toISOString();

export const randomSourceChannel = () =>
  faker.helpers.arrayElement(["API", "MOBILE_APP", "WEB_PORTAL"]);
export const randomDeviceType = () =>
  faker.helpers.arrayElement(["ANDROID", "IOS", "DESKTOP"]);

export async function reloadDimensionTables() {
  const client = await pgPool.connect();
  try {
    const m = await client.query("SELECT * FROM merchants");
    merchants = m.rows;

    const a = await client.query("SELECT * FROM accounts");
    accounts = a.rows;
  } finally {
    client.release();
  }
}
