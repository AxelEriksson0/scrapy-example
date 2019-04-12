from spidermon import Monitor, MonitorSuite, monitors
from spidermon.contrib.actions.slack.notifiers import SendSlackMessageSpiderFinished

@monitors.name('Periodic job stats monitor')
class PeriodicJobStatsMonitor(Monitor):

    @monitors.name('Maximum number of errors reached')
    def test_number_of_errors(self):
        accepted_errors = 0
        number_of_errors = self.data.stats.get('log_count/ERROR', 0)

        message = 'The job has exceeded the maximum number of errors'
        self.assertLessEqual(number_of_errors, accepted_errors, msg = message)

@monitors.name('Item count')
class ItemCountMonitor(Monitor):

    @monitors.name('Minimum number of items')
    def test_minimum_number_of_items(self):

        items_extracted = getattr(self.data.stats, 'item_scraped_count', 0)

        minimum_number_of_items = 1

        message = 'Extracted less than {} items'.format(minimum_number_of_items)

        self.assertTrue(items_extracted >= minimum_number_of_items, msg = message)

class PeriodicMonitorSuite(MonitorSuite):
    monitors = [
        PeriodicJobStatsMonitor,
    ]

    monitors_failed_actions = [
        SendSlackMessageSpiderFinished,
    ]

class SpiderCloseMonitorSuite(MonitorSuite):

    monitors = [
        ItemCountMonitor,
    ]

    monitors_failed_actions = [
        SendSlackMessageSpiderFinished,
    ]
