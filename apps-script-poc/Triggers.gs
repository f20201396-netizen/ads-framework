/**
 * Triggers.gs — orchestrator + recurring trigger management.
 *
 * runPoc(): one full cycle — Meta spend ingest → BQ attribution → aggregate
 * → write test tab.
 *
 * installTrigger() / uninstallTrigger(): manage the 30-min recurring trigger.
 */

const Triggers = (function () {
  const HANDLER = 'runPoc';

  /** Main entry point. Safe to run manually from the editor. */
  function runPoc() {
    const t0 = Date.now();
    Logger.log('=== runPoc start ===');

    // Step 1 — ingest last 3 days of campaign spend (idempotent upsert)
    const spendStats = IngestSpendCampaign.run(3);
    Logger.log('Spend ingest: ' + JSON.stringify(spendStats) + ' (' + _elapsed(t0) + 's)');

    // Step 2-4 — attribution + aggregate + write
    const rows = Aggregate.buildCampaignLevel();
    Logger.log('Aggregated ' + rows.length + ' campaigns (' + _elapsed(t0) + 's)');

    const written = WriteSheet.write(rows);
    Logger.log('=== runPoc done: ' + written + ' rows in ' + _elapsed(t0) + 's ===');
    return { campaigns: written, elapsedSeconds: _elapsed(t0) };
  }

  /** Cold-start: pull 90 days of campaign spend then run a full cycle. */
  function coldStart() {
    Logger.log('Cold-start: pulling 90 days of campaign spend...');
    IngestSpendCampaign.coldStart();
    return runPoc();
  }

  /** Install a 30-min recurring time trigger. Idempotent. */
  function installTrigger() {
    uninstallTrigger();
    ScriptApp.newTrigger(HANDLER).timeBased().everyMinutes(30).create();
    Logger.log('Installed 30-min trigger for ' + HANDLER);
  }

  function uninstallTrigger() {
    const triggers = ScriptApp.getProjectTriggers();
    let removed = 0;
    triggers.forEach(function (t) {
      if (t.getHandlerFunction() === HANDLER) {
        ScriptApp.deleteTrigger(t);
        removed++;
      }
    });
    Logger.log('Removed ' + removed + ' existing triggers');
  }

  function _elapsed(t0) {
    return Math.round((Date.now() - t0) / 100) / 10;
  }

  return { runPoc, coldStart, installTrigger, uninstallTrigger };
})();

// Top-level wrapper so the trigger system finds the handler by name.
function runPoc() { return Triggers.runPoc(); }
function coldStart() { return Triggers.coldStart(); }
function installTrigger() { return Triggers.installTrigger(); }
function uninstallTrigger() { return Triggers.uninstallTrigger(); }
