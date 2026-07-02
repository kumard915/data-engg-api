import fs from 'fs-extra';
import dayjs from 'dayjs';
import { generateMerchants } from './generators/merchant.js';
import { generateAccounts } from './generators/account.js';
import { generatePayins } from './generators/payin.js';
import { generatePayouts } from './generators/payout.js';
import { writeJsonAndCsv } from './utils/helpers.js';

const startDate = dayjs('2025-08-20');
const endDate = dayjs(); // today

let merchants = [];
let accounts = [];

async function run() {
  for (let d = startDate; d.isBefore(endDate) || d.isSame(endDate); d = d.add(1, 'day')) {
    const dateStr = d.format('YYYY-MM-DD');
    const dir = `./data/${dateStr}`;
    await fs.ensureDir(dir);

    // update dimensions
    merchants = generateMerchants(merchants, dateStr);
    accounts = generateAccounts(accounts, dateStr);

    // calculate counts with daily growth
    const daysPassed = d.diff(startDate, 'day');
    const payinCount = 2000 + daysPassed * 100;
    const payoutCount = 1000 + daysPassed * 50;

    const payins = generatePayins(dateStr, merchants, accounts, payinCount, true);
    const payouts = generatePayouts(dateStr, merchants, accounts, payoutCount, true);

    await writeJsonAndCsv(dir, 'merchants', merchants);
    await writeJsonAndCsv(dir, 'accounts', accounts);
    await writeJsonAndCsv(dir, 'payins', payins);
    await writeJsonAndCsv(dir, 'payouts', payouts);

    await fs.writeJson(`${dir}/meta.json`, { merchants: merchants.length, accounts: accounts.length, payins: payinCount, payouts: payoutCount }, { spaces: 2 });

    console.log(`✅ ${dateStr} -> Payins: ${payinCount}, Payouts: ${payoutCount}, Merchants: ${merchants.length}, Accounts: ${accounts.length}`);
  }
}

run().catch(err => { console.error(err); process.exit(1); });
