/**
 * Smoke.gs — quick checks run from the Apps Script editor after first deploy.
 */

const Smoke = (function () {

  function testBq() {
    const rows = BqClient.query('SELECT 1 AS one', { skipDryRun: true });
    Logger.log('BQ smoke: ' + JSON.stringify(rows));
    return rows;
  }

  function testMeta() {
    const cfg = Config.getAll();
    const acct = cfg.adAccountIds[0];
    const r = MetaClient.getCampaignInsights(acct, _yesterday());
    Logger.log('Meta smoke for ' + acct + ': ' + r.length + ' campaign rows yesterday');
    return r.slice(0, 3);
  }

  function testAttribution() {
    const map = FetchAttribution.run();
    const cids = Object.keys(map);
    Logger.log('Attribution smoke: ' + cids.length + ' campaigns');
    if (cids.length > 0) {
      Logger.log('Sample: ' + JSON.stringify(map[cids[0]]));
    }
    return cids.length;
  }

  function testFullCycle() {
    return Triggers.runPoc();
  }

  function _yesterday() {
    const d = new Date();
    d.setUTCDate(d.getUTCDate() - 1);
    return Utilities.formatDate(d, 'UTC', 'yyyy-MM-dd');
  }

  return { testBq, testMeta, testAttribution, testFullCycle };
})();

function testBq()          { return Smoke.testBq(); }
function testMeta()        { return Smoke.testMeta(); }
function testAttribution() { return Smoke.testAttribution(); }
function testFullCycle()   { return Smoke.testFullCycle(); }
