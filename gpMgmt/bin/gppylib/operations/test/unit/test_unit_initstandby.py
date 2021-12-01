#!/usr/bin/env python3

import os
import unittest

from gppylib.mainUtils import ExceptionNoStackTraceNeeded
from gppylib.operations.initstandby import get_standby_pg_hba_info, update_pg_hba
from gppylib.operations.update_pg_hba_on_segments import update_pg_hba_on_segments
from mock import MagicMock, Mock, mock_open, patch

class InitStandbyTestCase(unittest.TestCase):

    @patch('gppylib.operations.initstandby.gp.IfAddrs.list_addrs', return_value=['192.168.2.1', '192.168.1.1'])
    @patch('gppylib.operations.initstandby.unix.UserId.local', return_value='all')
    def test_get_standby_pg_hba_info(self, m1, m2):
        expected = '# standby coordinator host ip addresses\nhost\tall\tall\t192.168.2.1/32\ttrust\nhost\tall\tall\t192.168.1.1/32\ttrust\n'
        self.assertEqual(expected, get_standby_pg_hba_info('host'))

    def test_update_pg_hba(self):
        file_contents = """some pg hba data here\n"""
        pg_hba_info = '# standby coordinator host ip addresses\nhost\tall\tall\t192.168.2.1/32\ttrust\nhost\tall\tall\t192.168.1.1/32\ttrust\n'
        data_dirs = ['/tmp/d1', '/tmp/d2']
        expected = [file_contents + pg_hba_info, file_contents + pg_hba_info]
        m = MagicMock()
        m.return_value.__enter__.return_value.read.side_effect = [file_contents, file_contents]
        with patch('builtins.open', m, create=True):
            self.assertEqual(expected, update_pg_hba(pg_hba_info, data_dirs))

    def test_update_pg_hba_duplicate(self):
        file_contents = """some pg hba data here\n"""
        duplicate_entry = """# standby coordinator host ip addresses\nhost\tall\tall\t192.168.2.1/32\ttrust\nhost\tall\tall\t192.168.1.1/32\ttrust\n"""
        pg_hba_info = '# standby coordinator host ip addresses\nhost\tall\tall\t192.168.2.1/32\ttrust\nhost\tall\tall\t192.168.1.1/32\ttrust\n'
        data_dirs = ['/tmp/d1', '/tmp/d2']
        expected = [file_contents + pg_hba_info]
        m = MagicMock()
        m.return_value.__enter__.return_value.read.side_effect = [file_contents, file_contents + duplicate_entry]
        with patch('builtins.open', m, create=True):
            res = update_pg_hba(pg_hba_info, data_dirs)
            self.assertEqual(expected, res) 

    @patch('gppylib.operations.initstandby.WorkerPool')
    @patch('gppylib.operations.initstandby.get_standby_pg_hba_info', return_value='standby ip')
    def test_update_pg_hba_on_segments(self, m1, m2):
        mock_segs = []
        batch_size = 1
        for i in range(6):
            m = Mock()
            m.getSegmentContentId = Mock()
            m.getSegmentContentId.return_value = (i % 3) + 1
            m.getSegmentDataDirectory.return_value = '/tmp/d%d' % i
            mock_segs.append(m)
        gparray = Mock()
        gparray.getSegmentList = Mock()
        gparray.getSegmentList.return_value = mock_segs
        update_pg_hba_on_segments(gparray, 'standby_host', batch_size)

    @patch('gppylib.operations.update_pg_hba_on_segments.create_entries')
    @patch('gppylib.operations.update_pg_hba_on_segments.SegUpdateHba')
    def test_update_pg_hba_on_unreachable_segments(self, m1, m2):
        mock_segs = []
        batch_size = 1
        for i in range(6):
            m = Mock()
            m.getSegmentContentId = Mock()
            m.getSegmentContentId.return_value = (i % 3) + 1
            m.getSegmentDataDirectory.return_value = '/tmp/d%d' % i
            m.primaryDB = Mock()
            m.primaryDB.unreachable = False
            m.primaryDB.getSegmentHostName = Mock()
            m.primaryDB.getSegmentHostName.return_value = 'seg%d' % i
            m.mirrorDB = Mock()
            m.mirrorDB.unreachable = False
            m.mirrorDB.getSegmentHostName = Mock()
            m.mirrorDB.getSegmentHostName.return_value = 'mir%d' % i
            mock_segs.append(m)
        gparray = Mock()
        gparray.getSegmentList = Mock()
        gparray.getSegmentList.return_value = mock_segs

        #check when one of the mirror node is unavailable
        mock_segs[0].mirrorDB = None
        try:
            update_pg_hba_on_segments(gparray, 'standby_host', batch_size)
        except AttributeError as e:
            self.assertTrue(False)
        mock_segs[0].mirrorDB = Mock()

        # no warning message when all nodes are reachable
        with self.assertNoLogs(level='WARNING'):
            update_pg_hba_on_segments(gparray, 'standby_host', batch_size)

        # warning message when one of the primary node is not reachable
        with self.assertLogs(level='WARNING') as cm:
            update_pg_hba_on_segments(gparray, 'standby_host', batch_size, ['seg1'])
            self.assertEqual(cm.output[0],
                             'WARNING:default:Manual update of the pg_hba_conf files for all segments on unreachable host seg1 will be required.')

        # warning message when one of the mirror node is not reachable
        with self.assertLogs(level='WARNING') as cm:
            update_pg_hba_on_segments(gparray, 'standby_host', batch_size, ['mir1'])
            self.assertEqual(cm.output[0],
                             'WARNING:default:Manual update of the pg_hba_conf files for all segments on unreachable host mir1 will be required.')

        # warning message when both primary and mirror node is not reachable
        with self.assertLogs(level='WARNING') as cm:
            update_pg_hba_on_segments(gparray, 'standby_host', batch_size, ['seg1', 'mir1'])
            self.assertEqual(cm.output[0],
                             'WARNING:default:Manual update of the pg_hba_conf files for all segments on unreachable host seg1 will be required.')
            self.assertEqual(cm.output[1],
                             'WARNING:default:Manual update of the pg_hba_conf files for all segments on unreachable host mir1 will be required.')

