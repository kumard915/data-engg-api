import fs from 'fs-extra';
import { createObjectCsvWriter } from 'csv-writer';

export async function writeJsonAndCsv(dir, filename, arr) {
  const jsonPath = `${dir}/${filename}.json`;
  const csvPath = `${dir}/${filename}.csv`;
  await fs.writeJson(jsonPath, arr, { spaces: 2 });

  if (!arr || arr.length === 0) {
    await fs.writeFile(csvPath, '');
    return;
  }

  const rows = arr.map(o => {
    const flat = {};
    for (const k of Object.keys(o)) {
      const v = o[k];
      if (v === null || typeof v === 'undefined') flat[k] = '';
      else if (typeof v === 'object') flat[k] = JSON.stringify(v);
      else flat[k] = v;
    }
    return flat;
  });

  const headers = Object.keys(rows[0]).map(h => ({id: h, title: h}));
  const csvWriter = createObjectCsvWriter({ path: csvPath, header: headers });
  await csvWriter.writeRecords(rows);
}
