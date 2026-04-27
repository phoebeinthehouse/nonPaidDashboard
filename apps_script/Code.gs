/**
 * Non-Paid Video Dashboard — Apps Script backend
 *
 * Reads from the source tracking sheet, optionally maintains a daily history
 * tab, and serves the HTML dashboard as a web app.
 */

const SHEET_ID       = '1mqtqPdEO0WqVWTFd6f2nQzCLp-KjWcrziyaOUDl0lGY';
const SOURCE_TAB     = '오가닉 + 무가시딩 트레킹';
const HISTORY_TAB    = '_history';
const DATA_START_ROW = 5;
const TZ             = 'Asia/Seoul';

// ─── Web app entrypoint ──────────────────────────────────────────────────────
function doGet() {
  return HtmlService.createHtmlOutputFromFile('Index')
    .setTitle('Non-Paid Video Dashboard')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL);
}

// ─── Read current snapshot + history, return to client ───────────────────────
function getDashboardData() {
  const ss = SpreadsheetApp.openById(SHEET_ID);
  const sheet = ss.getSheetByName(SOURCE_TAB);
  const lastRow = sheet.getLastRow();
  if (lastRow < DATA_START_ROW) return { today: [], dates: [] };

  const data = sheet.getRange(DATA_START_ROW, 1, lastRow - DATA_START_ROW + 1, 15).getValues();

  const today = data
    .map(r => ({
      type:         String(r[3] || ''),
      handle:       String(r[4] || ''),
      followers:    parseInt(r[5]) || 0,
      tier:         String(r[6] || ''),
      uploadedDate: r[7] instanceof Date ? Utilities.formatDate(r[7], TZ, 'yyyy-MM-dd') : String(r[7] || ''),
      product:      String(r[8] || ''),
      channel:      String(r[9] || ''),
      url:          String(r[10] || ''),
      views:        parseInt(String(r[11]).toString().replace(/,/g, '')) || 0,
      likes:        parseInt(String(r[12]).toString().replace(/,/g, '')) || 0,
      comments:     parseInt(String(r[13]).toString().replace(/,/g, '')) || 0,
      shares:       parseInt(String(r[14]).toString().replace(/,/g, '')) || 0,
    }))
    .filter(r => r.url && (r.url.indexOf('tiktok.com') >= 0 || r.url.indexOf('instagram.com') >= 0));

  // Engagement rate
  today.forEach(r => {
    r.er = r.views > 0 ? +(((r.likes + r.comments + r.shares) / r.views) * 100).toFixed(2) : 0;
  });

  // Pull history if it exists, build url -> [{date, views}]
  const history = {};
  let dates = [];
  const histSheet = ss.getSheetByName(HISTORY_TAB);
  if (histSheet && histSheet.getLastRow() > 1) {
    const histData = histSheet.getRange(2, 1, histSheet.getLastRow() - 1, 3).getValues();
    const dateSet = {};
    histData.forEach(row => {
      const date = row[0] instanceof Date
        ? Utilities.formatDate(row[0], TZ, 'yyyy-MM-dd')
        : String(row[0]);
      const url = String(row[1] || '');
      const views = parseInt(row[2]) || 0;
      if (!url) return;
      if (!history[url]) history[url] = [];
      history[url].push({ date: date, views: views });
      dateSet[date] = true;
    });
    dates = Object.keys(dateSet).sort();
    Object.keys(history).forEach(url => history[url].sort((a, b) => a.date.localeCompare(b.date)));
  }

  // Compute deltaViews vs. last snapshot
  today.forEach(r => {
    const hist = history[r.url] || [];
    if (hist.length) {
      r.deltaViews = r.views - hist[hist.length - 1].views;
    } else {
      r.deltaViews = null;
    }
    r.trend = r.deltaViews == null ? '–' :
              r.deltaViews > 0 ? '📈 Growing' :
              r.deltaViews < 0 ? '📉 Dropping' : '➡ Stable';
  });

  return {
    today:     today,
    history:   history,
    dates:     dates,
    asOf:      Utilities.formatDate(new Date(), TZ, 'yyyy-MM-dd HH:mm'),
  };
}

// ─── Daily snapshot — run on a time-based trigger ────────────────────────────
function snapshotDaily() {
  const ss = SpreadsheetApp.openById(SHEET_ID);
  let histSheet = ss.getSheetByName(HISTORY_TAB);
  if (!histSheet) {
    histSheet = ss.insertSheet(HISTORY_TAB);
    histSheet.appendRow(['date', 'url', 'views', 'likes', 'comments', 'shares']);
    histSheet.setFrozenRows(1);
  }

  const sheet = ss.getSheetByName(SOURCE_TAB);
  const lastRow = sheet.getLastRow();
  if (lastRow < DATA_START_ROW) return;

  const data = sheet.getRange(DATA_START_ROW, 1, lastRow - DATA_START_ROW + 1, 15).getValues();
  const today = Utilities.formatDate(new Date(), TZ, 'yyyy-MM-dd');

  const rows = data
    .filter(r => r[10] && (String(r[10]).indexOf('tiktok.com') >= 0 || String(r[10]).indexOf('instagram.com') >= 0))
    .map(r => [
      today,
      String(r[10]),
      parseInt(String(r[11]).toString().replace(/,/g, '')) || 0,
      parseInt(String(r[12]).toString().replace(/,/g, '')) || 0,
      parseInt(String(r[13]).toString().replace(/,/g, '')) || 0,
      parseInt(String(r[14]).toString().replace(/,/g, '')) || 0,
    ]);

  if (rows.length) {
    histSheet.getRange(histSheet.getLastRow() + 1, 1, rows.length, 6).setValues(rows);
  }
  Logger.log('Snapshot saved: ' + rows.length + ' videos for ' + today);
}

// ─── One-time helper: install a daily 9 AM trigger ───────────────────────────
function installDailyTrigger() {
  ScriptApp.getProjectTriggers()
    .filter(t => t.getHandlerFunction() === 'snapshotDaily')
    .forEach(t => ScriptApp.deleteTrigger(t));

  ScriptApp.newTrigger('snapshotDaily')
    .timeBased()
    .atHour(9)
    .everyDays(1)
    .inTimezone(TZ)
    .create();
  Logger.log('Daily snapshot trigger installed for 9 AM ' + TZ);
}
