import copy
import json
import unittest
from types import SimpleNamespace
from review_factory_resumption_parity import MARKER, extract, snapshot_live_counts, validate


class FactoryReceiptGuards(unittest.TestCase):
    def probe(self):
        return {'paused': {'state': 'paused'}, 'done': {'state': 'done', 'nodes': [
            {'id': 'work', 'status': 'done', 'entries_used': 1, 'attempts': 2},
            {'id': 'verified', 'status': 'done', 'entries_used': 1, 'attempts': 1}]}}

    def test_rejects_missing_resume_extra_entry_or_extra_attempt(self):
        validate(self.probe())
        for key, value in [('entries_used', 2), ('attempts', 1), ('attempts', 3), ('status', 'error')]:
            probe = self.probe()
            probe['done']['nodes'][0][key] = value
            with self.assertRaises(AssertionError):
                validate(probe)

    def test_accepts_explicit_wire_camel_case_without_ignoring_duplicates(self):
        probe = self.probe()
        for row in probe['done']['nodes']:
            row['entriesUsed'] = row.pop('entries_used')
        validate(probe)
        probe['done']['nodes'][0]['entries_used'] = 1
        with self.assertRaises(AssertionError):
            validate(probe)

    def test_rejects_missing_or_historical_tool_receipt(self):
        probe = self.probe()
        tool = {'role': 'tool', 'tool_call_id': 'factory_probe',
                'content': MARKER + json.dumps(probe)}
        rows = [{}, {'messages': [tool]}]
        self.assertEqual(extract(rows), probe)
        for changed in [[{'messages': [tool]}, {'messages': []}],
                        [{}, {'messages': [tool, copy.deepcopy(tool)]}],
                        [{}, {'messages': [{**tool, 'tool_call_id': 'historical'}]}]]:
            with self.assertRaises(AssertionError):
                extract(changed)


class LiveCountSnapshotGuard(unittest.TestCase):
    def test_snapshot_copies_arrivals_and_reads_counters_at_one_instant(self):
        server = SimpleNamespace(requests=[1, 2], parent_requests=[1],
                                 child_requests=[1, 2],
                                 request_arrivals=[(10.0, 'parent'), (11.0, 'child'), (12.0, 'child')])
        clock = iter([42.0])
        snapshot = snapshot_live_counts(server, clock=lambda: next(clock))
        self.assertEqual(snapshot, {
            'total_requests': 2,
            'parent_requests': 1,
            'child_requests': 2,
            'request_arrivals': [(10.0, 'parent'), (11.0, 'child'), (12.0, 'child')],
            'snapshot_time': 42.0,
        })
        # The arrival inventory is a copy: later requests never rewrite history.
        server.request_arrivals.append((13.0, 'child'))
        self.assertEqual(len(snapshot['request_arrivals']), 3)


if __name__ == '__main__':
    unittest.main()
