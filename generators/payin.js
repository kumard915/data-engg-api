import dayjs from "dayjs";
import utc from "dayjs/plugin/utc.js";
import { faker } from "@faker-js/faker";
import { nanoid } from "nanoid";
import {
  randomBank,
  randomZone,
  randomStatus,
  randomAmount,
  randomSourceChannel,
  randomDeviceType,
} from "../utils/faker.js";

dayjs.extend(utc);

export function generatePayins(
  date, // kept for backward compatibility
  merchantList,
  accountList,
  count,
  extended = false
) {
  const payins = [];

  const merchants = Array.isArray(merchantList)
    ? merchantList.filter(Boolean)
    : [];
  const accounts = Array.isArray(accountList)
    ? accountList.filter(Boolean)
    : [];

  const defaultMerchant = { id: "M-DEFAULT" };
  const defaultAccount = {
    accNum: "0000000000",
    vpa: "default@okbizaxis",
    bank: randomBank(),
    zone: randomZone(),
  };

  for (let i = 0; i < count; i++) {
    const merchant = merchants.length
      ? faker.helpers.arrayElement(merchants)
      : defaultMerchant;

    const account = accounts.length
      ? faker.helpers.arrayElement(accounts)
      : defaultAccount;

    // ✅ Always real-time UTC (NO 23:59 bug anymore)
    const created = dayjs().utc().toISOString();

    const amount = randomAmount();
    const status = randomStatus();

    const processedOn = dayjs(created)
      .add(faker.number.int({ min: 1, max: 60 }), "second")
      .toISOString();

    const base = {
      id: `Q-${nanoid(16)}`,
      seqNum: faker.number.int({ min: 1000, max: 999999 }),
      merchantId: merchant.id,
      requestId: faker.string.uuid(),
      customer: {
        name: faker.person.firstName(),
        mobile: faker.phone.number("9#########"),
        email: faker.internet.email(),
      },
      transaction: {
        amount,
        type: faker.helpers.arrayElement(["BASIC", "AUTO", "CONFIRM"]),
        callbackUrl: faker.internet.url(),
      },
      orderCode: faker.string.alphanumeric(6),
      receivingVpa: account.vpa,
      bank: account.bank,
      zone: account.zone,
      utr: faker.string.alphanumeric(20),
      processedAmount: 0,
      status,
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
      base.deviceType = randomDeviceType();
      base.retryCount = faker.number.int({ min: 0, max: 2 });
      base.isHighValue = amount > 10000;
      base.successFlag = status.startsWith("SUCCESS");
    }

    payins.push(base);
  }

  return payins;
}

// import { faker } from "@faker-js/faker";
// import { nanoid } from "nanoid";
// import {
//   randomBank,
//   randomZone,
//   randomStatus,
//   randomAmount,
//   randomDateTime,
//   randomSourceChannel,
//   randomDeviceType,
// } from "../utils/faker.js";

// export function generatePayins(
//   date,
//   merchantList,
//   accountList,
//   count,
//   extended = false
// ) {
//   const payins = [];
//   const merchants = Array.isArray(merchantList)
//     ? merchantList.filter(Boolean)
//     : [];
//   const accounts = Array.isArray(accountList)
//     ? accountList.filter(Boolean)
//     : [];

//   const defaultMerchant = { id: "M-DEFAULT", name: "Default Merchant" };
//   const defaultAccount = {
//     accNum: "0000000000",
//     vpa: "default@okbizaxis",
//     bank: randomBank(),
//     zone: randomZone(),
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
//     const created = randomDateTime(date);
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
//       id: `Q-${nanoid(16)}`,
//       seqNum: faker.number.int({ min: 1000, max: 999999 }),
//       merchantId: merchant?.id || defaultMerchant.id,
//       requestId: faker.string.uuid(),
//       customer: {
//         name: faker.person.firstName(),
//         mobile: faker.phone.number("9#########"),
//         email: faker.internet.email(),
//       },
//       transaction: {
//         amount: amount,
//         type: faker.helpers.arrayElement(["BASIC", "AUTO", "CONFIRM"]),
//         callbackUrl: faker.internet.url(),
//       },
//       orderCode: faker.string.alphanumeric(6),
//       receivingVpa: account?.vpa || defaultAccount.vpa,
//       bank: account?.bank || defaultAccount.bank,
//       zone: account?.zone || defaultAccount.zone,
//       utr: faker.string.alphanumeric(20),
//       processedAmount: 0,
//       status: status,
//       createdOn: created,
//       updatedOn: created,
//     };

//     if (extended) {
//       base.processedOn = processedOn;
//       base.processingTimeSec = processingTimeSec;
//       base.sourceChannel = randomSourceChannel();
//       base.deviceType = randomDeviceType();
//       base.retryCount = faker.number.int({ min: 0, max: 2 });
//       base.isHighValue = amount > 10000;
//       base.successFlag = status.startsWith("SUCCESS");
//     }

//     payins.push(base);
//   }

//   return payins;
// }
