import dayjs from "dayjs";
import utc from "dayjs/plugin/utc.js";
import { faker } from "@faker-js/faker";
import { nanoid } from "nanoid";
import {
  randomBank,
  randomStatus,
  randomAmount,
  randomSourceChannel,
} from "../utils/faker.js";

dayjs.extend(utc);

export function generatePayouts(
  date, // kept for backward compatibility
  merchantList,
  accountList,
  count,
  extended = false
) {
  const payouts = [];

  const merchants = Array.isArray(merchantList)
    ? merchantList.filter(Boolean)
    : [];
  const accounts = Array.isArray(accountList)
    ? accountList.filter(Boolean)
    : [];

  const defaultMerchant = { id: "M-DEFAULT" };
  const defaultAccount = {
    accNum: "0000000000",
    bank: randomBank(),
    ifsc: "DEFAULTIFSC",
  };

  for (let i = 0; i < count; i++) {
    const merchant = merchants.length
      ? faker.helpers.arrayElement(merchants)
      : defaultMerchant;

    const account = accounts.length
      ? faker.helpers.arrayElement(accounts)
      : defaultAccount;

    // ✅ Same fix as payins → real-time UTC
    const created = dayjs().utc().toISOString();

    const amount = randomAmount();
    const status = randomStatus();

    const processedOn = dayjs(created)
      .add(faker.number.int({ min: 1, max: 60 }), "second")
      .toISOString();

    const base = {
      id: `Q-${nanoid(12)}`,
      seqNum: faker.number.int({ min: 1000, max: 999999 }),
      merchantId: merchant.id,

      transaction: {
        amount,
        type: faker.helpers.arrayElement(["IMPS", "NEFT"]),
      },

      account: {
        bank: account.bank,
        ifsc: account.ifsc,
        account: account.accNum,
      },

      status,
      requestedAmount: amount,
      processedAmount: 0,
      utr: faker.string.numeric(10),

      createdOn: created,
      updatedOn: created,
    };

    if (extended) {
      base.processedOn = processedOn;
      base.processingTimeSec = dayjs(processedOn).diff(
        dayjs(created),
        "second"
      );
      base.sourceChannel = randomSourceChannel();
      base.retryCount = faker.number.int({ min: 0, max: 2 });
      base.successFlag = status.startsWith("SUCCESS");
    }

    payouts.push(base);
  }

  return payouts;
}

// import { faker } from "@faker-js/faker";
// import { nanoid } from "nanoid";
// import dayjs from "dayjs";

// import {
//   randomBank,
//   randomZone,
//   randomStatus,
//   randomAmount,
//   randomDateTime,
//   randomSourceChannel,
// } from "../utils/faker.js";

// export function generatePayouts(
//   date,
//   merchantList,
//   accountList,
//   count,
//   extended = false
// ) {
//   const payouts = [];
//   const merchants = Array.isArray(merchantList)
//     ? merchantList.filter(Boolean)
//     : [];
//   const accounts = Array.isArray(accountList)
//     ? accountList.filter(Boolean)
//     : [];

//   const defaultMerchant = { id: "M-DEFAULT", name: "Default Merchant" };
//   const defaultAccount = {
//     accNum: "0000000000",
//     bank: randomBank(),
//     ifsc: "DEFAULTIFSC",
//   };

//   for (let i = 0; i < count; i++) {
//     const merchant =
//       merchants.length > 0
//         ? faker.helpers.arrayElement(merchants)
//         : defaultMerchant;
//     const account =
//       accounts.length > 0
//         ? faker.helpers.arrayElement(accounts)
//         : defaultAccount;
//     // const created = randomDateTime(date);
//     const created = dayjs().utc().toISOString();

//     const amount = randomAmount();
//     const status = randomStatus();
//     const processedOn = new Date(
//       new Date(created).getTime() + faker.number.int({ min: 1000, max: 60000 })
//     ).toISOString();
//     const processingTimeSec = Math.max(
//       1,
//       Math.round((new Date(processedOn) - new Date(created)) / 1000)
//     );

//     const base = {
//       id: `Q-${nanoid(12)}`,
//       seqNum: faker.number.int({ min: 1000, max: 999999 }),
//       merchantId: merchant?.id || defaultMerchant.id,
//       transaction: {
//         amount: amount,
//         type: faker.helpers.arrayElement(["IMPS", "NEFT"]),
//       },
//       account: {
//         bank: account?.bank || defaultAccount.bank,
//         ifsc: account?.ifsc || defaultAccount.ifsc,
//         account: account?.accNum || defaultAccount.accNum,
//       },
//       status: status,
//       requestedAmount: amount,
//       processedAmount: 0,
//       utr: faker.string.numeric(10),
//       createdOn: created,
//       updatedOn: created,
//     };

//     if (extended) {
//       base.processedOn = processedOn;
//       base.processingTimeSec = processingTimeSec;
//       base.sourceChannel = randomSourceChannel();
//       base.retryCount = faker.number.int({ min: 0, max: 2 });
//       base.successFlag = status.startsWith("SUCCESS");
//     }

//     payouts.push(base);
//   }

//   return payouts;
// }
