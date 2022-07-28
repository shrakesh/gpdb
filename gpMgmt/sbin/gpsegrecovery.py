#!/usr/bin/env python3
import datetime

from gppylib.recoveryinfo import RecoveryErrorType
from gppylib.commands.pg import PgBaseBackup, PgRewind

from gppylib.db import dbconn
from recovery_base import RecoveryBase, set_recovery_cmd_results
from gppylib.commands.base import Command
from gppylib.commands.gp import SegmentStart
from gppylib.gparray import Segment
from gppylib.commands.gp import ModifyConfSetting


class FullRecovery(Command):
    def __init__(self, name, recovery_info, forceoverwrite, logger, era):
        self.name = name
        self.recovery_info = recovery_info
        self.replicationSlotName = 'internal_wal_replication_slot'
        self.forceoverwrite = forceoverwrite
        self.era = era
        # FIXME test for this cmdstr. also what should this cmdstr be ?
        cmdStr = ''
        #cmdstr = 'TODO? : {} {}'.format(str(recovery_info), self.verbose)
        Command.__init__(self, self.name, cmdStr)
        #FIXME this logger has to come after the init and is duplicated in all the 4 classes
        self.logger = logger
        self.error_type = RecoveryErrorType.DEFAULT_ERROR

    @set_recovery_cmd_results
    def run(self):

        #####
        # import pdb; pdb.set_trace()
        # datadir = get_data_dir(self.recovery_info.source_hostname,self.recovery_info.source_port)
        # if datadir is None:
        #     self.logger.info("not able to get source data dir")
        #
        # calculate_and_display_prediction(self.recovery_info.source_hostname, datadir)

        ####

        self.error_type = RecoveryErrorType.BASEBACKUP_ERROR
        cmd = PgBaseBackup(self.recovery_info.target_datadir,
                           self.recovery_info.source_hostname,
                           str(self.recovery_info.source_port),
                           create_slot=False,
                           replication_slot_name=self.replicationSlotName,
                           forceoverwrite=self.forceoverwrite,
                           target_gp_dbid=self.recovery_info.target_segment_dbid,
                           progress_file=self.recovery_info.progress_file)
        self.logger.info("Running pg_basebackup with progress output temporarily in %s" % self.recovery_info.progress_file)
        try:
            cmd.run(validateAfter=True)
        except Exception as e: #TODO should this be ExecutionError?
            self.logger.info("Running pg_basebackup failed: {}".format(str(e)))

            #  If the cluster never has mirrors, cmd will fail
            #  quickly because the internal slot doesn't exist.
            #  Re-run with `create_slot`.
            #  GPDB_12_MERGE_FIXME could we check it before? or let
            #  pg_basebackup create slot if not exists.
            cmd = PgBaseBackup(self.recovery_info.target_datadir,
                               self.recovery_info.source_hostname,
                               str(self.recovery_info.source_port),
                               create_slot=True,
                               replication_slot_name=self.replicationSlotName,
                               forceoverwrite=True,
                               target_gp_dbid=self.recovery_info.target_segment_dbid,
                               progress_file=self.recovery_info.progress_file)
            self.logger.info("Re-running pg_basebackup, creating the slot this time")
            cmd.run(validateAfter=True)

        self.error_type = RecoveryErrorType.DEFAULT_ERROR
        self.logger.info("Successfully ran pg_basebackup for dbid: {}".format(
            self.recovery_info.target_segment_dbid))

        # Updating port number on conf after recovery
        self.error_type = RecoveryErrorType.UPDATE_ERROR
        update_port_in_conf(self.recovery_info, self.logger)

        self.error_type = RecoveryErrorType.START_ERROR
        start_segment(self.recovery_info, self.logger, self.era)


class IncrementalRecovery(Command):
    def __init__(self, name, recovery_info, logger, era):
        self.name = name
        self.recovery_info = recovery_info
        self.era = era
        cmdStr = ''
        Command.__init__(self, self.name, cmdStr)
        self.logger = logger
        self.error_type = RecoveryErrorType.DEFAULT_ERROR

    @set_recovery_cmd_results
    def run(self):

        #####
        # import pdb; pdb.set_trace()
        # datadir = get_data_dir(self.recovery_info.source_hostname,self.recovery_info.source_port)
        # if datadir is None:
        #     self.logger.info("not able to get source data dir")
        #
        # calculate_and_display_prediction(self.recovery_info.source_hostname, datadir)

        ####

        self.logger.info("Running pg_rewind with progress output temporarily in %s" % self.recovery_info.progress_file)
        self.error_type = RecoveryErrorType.REWIND_ERROR
        cmd = PgRewind('rewind dbid: {}'.format(self.recovery_info.target_segment_dbid),
                       self.recovery_info.target_datadir, self.recovery_info.source_hostname,
                       self.recovery_info.source_port, self.recovery_info.progress_file)
        cmd.run(validateAfter=True)
        self.logger.info("Successfully ran pg_rewind for dbid: {}".format(self.recovery_info.target_segment_dbid))

        # Updating port number on conf after recovery
        self.error_type = RecoveryErrorType.UPDATE_ERROR
        update_port_in_conf(self.recovery_info, self.logger)

        self.error_type = RecoveryErrorType.START_ERROR
        start_segment(self.recovery_info, self.logger, self.era)

#
# def get_network_speed(hostname):
#     cmd_str = "dd if=/dev/zero of=test bs=1024k count=200 2>/dev/null;rsync -av test %s:test|tail -2|head -1" % hostname
#     cmd = Command(name='Get-Network-Speed', cmdStr=cmd_str, ctxt=REMOTE, remoteHost=hostname)
#     cmd.run(validateAfter=True)
#     str_output = cmd.get_results().stdout
#     size = str_output.split(" ")
#     val = size[8]
#     unit = size[9]
#     return val,unit
#     #  print(val + " " + unit)
#     # sent 58060 bytes  received 101430 bytes  106326.67 bytes/sec
#
# def get_disc_speed(hostname):
#     cmd_str = "fio --end_fsync=1 --bs=1M --size=20G --rw=write --filename=zeroes --direct=1 --name=direct_1M --eta-newline=1 | tail -1"
#     cmd = Command(name='Get-disc-Speed', cmdStr=cmd_str, ctxt=REMOTE, remoteHost=hostname)
#     cmd.run(validateAfter=True)
#     str_output = cmd.get_results().stdout
#     size = str_output.split(" ")[2][1:-2]
#     unit = size[-4:]
#     val = size[:-4]
#     return val, unit
#    #  print(val + " " + unit)
#     #   WRITE: bw=1664MiB/s (1745MB/s), 1664MiB/s-1664MiB/s (1745MB/s-1745MB/s), io=20.0GiB (21.5GB), run=12309-12309msec
#
# def get_datadir_size(hostname, datadir):
#     cmd_str = "du -sh %s" % datadir
#     cmd = Command(name='Get-data-directory-size', cmdStr=cmd_str, ctxt=REMOTE, remoteHost=hostname)
#     cmd.run(validateAfter=True)
#     str_output = cmd.get_results().stdout
#     size = str_output.split("\t")[0] #this will give you size like 2.1M
#     unit = size[-1] #M or B or G
#     val = size[:-1] # val 2.1
#     return val, unit
#     #   2.1M	/tmp/
#
# def get_data_dir(hostname, port):
#     data_dir = None
#     try:
#         dburl = dbconn.DbURL()
#         conn = dbconn.connect(dburl, encoding='UTF8')
#         data_dir = dbconn.querySingleton(conn,
#                                       "select datadir from gp_segment_configuration where hostname = %s and  port = port;" %(hostname , port))
#     finally:
#         conn.close()
#
#     return data_dir
#
# def convertUnit(data, src, tgt='B'):
#     val = int (data)
#
#     res = 1
#     if src == 'K' or src == 'k':
#         res = val * 1024
#     if src == 'M' or src == 'm':
#         res = val * 1024 * 1024
#     if src == 'G' or src == 'g':
#         res = val * 1024 * 1024 * 1024
#
#     return res
#
#
# def calculate_and_display_prediction(hostname, datadir):
#     # get data speed divide by network speed - time in sec (data size/ n/w)
#     # get data speed divide by disc speed -  time in sec (
#     # add  both to get the total time in sec
#     network_speed, network_unit = get_network_speed(hostname)
#     disc_speed, disc_unit = get_disc_speed(hostname)
#     net_per_sec = convertUnit(network_speed, network_unit[0])
#     disc_per_sec = convertUnit(disc_speed, disc_unit[0])
#     dir_size, dir_unit = get_datadir_size(hostname, datadir)
#     dsize = convertUnit(dir_size, dir_unit[0])
#
#     # time in second:
#     time_over_network = int(dsize)/int(net_per_sec)
#     time_on_disc = int(dsize) / int(disc_per_sec)
#
#     expected_time = time_over_network + time_on_disc
#
#     print("expected time for completion: %s " % datetime.timedelta(expected_time))



def start_segment(recovery_info, logger, era):
    seg = Segment(None, None, None, None, None, None, None, None,
                  recovery_info.target_port, recovery_info.target_datadir)
    cmd = SegmentStart(
        name="Starting new segment with dbid %s:" % (str(recovery_info.target_segment_dbid))
        , gpdb=seg
        , numContentsInCluster=0
        , era=era
        , mirrormode="mirror"
        , utilityMode=True)
    logger.info(str(cmd))
    cmd.run(validateAfter=True)


def update_port_in_conf(recovery_info, logger):
    logger.info("Updating %s/postgresql.conf" % recovery_info.target_datadir)
    modifyConfCmd = ModifyConfSetting('Updating %s/postgresql.conf' % recovery_info.target_datadir,
                                      "{}/{}".format(recovery_info.target_datadir, 'postgresql.conf'),
                                      'port', recovery_info.target_port, optType='number')
    modifyConfCmd.run(validateAfter=True)


#FIXME we may not need this class
class SegRecovery(object):
    def __init__(self):
        pass

    def main(self):
        recovery_base = RecoveryBase(__file__)
        recovery_base.main(self.get_recovery_cmds(recovery_base.seg_recovery_info_list, recovery_base.options.forceoverwrite,
                                                  recovery_base.logger, recovery_base.options.era))

    def get_recovery_cmds(self, seg_recovery_info_list, forceoverwrite, logger, era):
        cmd_list = []
        for seg_recovery_info in seg_recovery_info_list:
            if seg_recovery_info.is_full_recovery:
                cmd = FullRecovery(name='Run pg_basebackup',
                                   recovery_info=seg_recovery_info,
                                   forceoverwrite=forceoverwrite,
                                   logger=logger,
                                   era=era)
            else:
                cmd = IncrementalRecovery(name='Run pg_rewind',
                                          recovery_info=seg_recovery_info,
                                          logger=logger,
                                          era=era)
            cmd_list.append(cmd)
        return cmd_list


if __name__ == '__main__':
    SegRecovery().main()
