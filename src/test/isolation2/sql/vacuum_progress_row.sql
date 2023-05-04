-- @Description Test to ensure we correctly report progress in
-- pg_stat_progress_create_vacuum for append-optimized tables.

set default_table_access_method=ao_row;

-- Setup the append-optimized table to be vacuumed
DROP TABLE IF EXISTS vacuum_progress_ao_row;
CREATE TABLE vacuum_progress_ao_row(i int, j int);

-- Add two indexes to be vacuumed as well
CREATE INDEX on vacuum_progress_ao_row(i);
CREATE INDEX on vacuum_progress_ao_row(j);

-- Insert from two current sessions so that data are stored in two segment files.
1: BEGIN;
2: BEGIN;
1: INSERT INTO vacuum_progress_ao_row SELECT i, i FROM generate_series(1, 100000) i;
2: INSERT INTO vacuum_progress_ao_row SELECT i, i FROM generate_series(1, 100000) i;
-- Commit so that the logical EOF of segno 2 is non-zero.
2: COMMIT;
2: BEGIN;
2: INSERT INTO vacuum_progress_ao_row SELECT i, i FROM generate_series(1, 100000) i;
-- Abort so that segno 2 has dead tuples after its logical EOF
2: ABORT;
2q:
-- Abort so that segno 1 has logical EOF = 0.
1: ABORT;

-- Also delete half of the tuples evenly before the EOF of segno 2.
DELETE FROM vacuum_progress_ao_row where j % 2 = 0;

-- Perform VACUUM and observe the progress

-- Suspend execution at pre-cleanup phase after truncating both segfiles to their logical EOF.
SELECT gp_inject_fault('appendonly_after_truncate_segment_file', 'suspend', '', '', '', 2, 2, 0, dbid) FROM gp_segment_configuration WHERE content > -1 AND role = 'p';

1: set Debug_appendonly_print_compaction to on;
1&: VACUUM vacuum_progress_ao_row;
SELECT gp_wait_until_triggered_fault('appendonly_after_truncate_segment_file', 2, dbid) FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
-- We are in pre_cleanup phase and some blocks should've been vacuumed by now
select relid::regclass as relname, phase, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count, max_dead_tuples, num_dead_tuples from gp_stat_progress_vacuum where gp_segment_id = 1;
select relid::regclass as relname, phase, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count, max_dead_tuples, num_dead_tuples from gp_stat_progress_vacuum_summary;

-- Resume execution and suspend again in the middle of compact phase
SELECT gp_inject_fault('appendonly_insert', 'suspend', '', '', '', 200, 200, 0, dbid) FROM gp_segment_configuration WHERE content > -1 AND role = 'p';
SELECT gp_inject_fault('appendonly_after_truncate_segment_file', 'reset', dbid) FROM gp_segment_configuration WHERE content > -1 AND role = 'p';
SELECT gp_wait_until_triggered_fault('appendonly_insert', 200, dbid) FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
-- We are in compact phase. num_dead_tuples should increase as we move and count tuples, one by one.
select relid::regclass as relname, phase, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count, max_dead_tuples, num_dead_tuples from gp_stat_progress_vacuum where gp_segment_id = 1;
select relid::regclass as relname, phase, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count, max_dead_tuples, num_dead_tuples from gp_stat_progress_vacuum_summary;

-- Resume execution and suspend again after compacting all segfiles
SELECT gp_inject_fault('vacuum_ao_after_compact', 'suspend', dbid) FROM gp_segment_configuration WHERE content > -1 AND role = 'p';
SELECT gp_inject_fault('appendonly_insert', 'reset', dbid) FROM gp_segment_configuration WHERE content > -1 AND role = 'p';
SELECT gp_wait_until_triggered_fault('vacuum_ao_after_compact', 1, dbid) FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
-- After compacting all segfiles we expect 50000 dead tuples
select relid::regclass as relname, phase, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count, max_dead_tuples, num_dead_tuples from gp_stat_progress_vacuum where gp_segment_id = 1;
select relid::regclass as relname, phase, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count, max_dead_tuples, num_dead_tuples from gp_stat_progress_vacuum_summary;

-- Resume execution and entering post_cleaup phase, suspend at the end of it.
SELECT gp_inject_fault('vacuum_ao_post_cleanup_end', 'suspend', dbid) FROM gp_segment_configuration WHERE content > -1 AND role = 'p';
SELECT gp_inject_fault('vacuum_ao_after_compact', 'reset', dbid) FROM gp_segment_configuration WHERE content > -1 AND role = 'p';
SELECT gp_wait_until_triggered_fault('vacuum_ao_post_cleanup_end', 1, dbid) FROM gp_segment_configuration WHERE content = 1 AND role = 'p';
-- We should have skipped recycling the awaiting drop segment because the segment was still visible to the SELECT gp_wait_until_triggered_fault query.
select relid::regclass as relname, phase, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count, max_dead_tuples, num_dead_tuples from gp_stat_progress_vacuum where gp_segment_id = 1;
select relid::regclass as relname, phase, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count, max_dead_tuples, num_dead_tuples from gp_stat_progress_vacuum_summary;

SELECT gp_inject_fault('vacuum_ao_post_cleanup_end', 'reset', dbid) FROM gp_segment_configuration WHERE content > -1 AND role = 'p';
1<:

-- pg_class and collected stats view should be updated after the 1st VACUUM
1U: SELECT wait_until_dead_tup_change_to('vacuum_progress_ao_row'::regclass::oid, 0);
SELECT relpages, reltuples, relallvisible FROM pg_class where relname = 'vacuum_progress_ao_row';
SELECT n_live_tup, n_dead_tup, last_vacuum is not null as has_last_vacuum, vacuum_count FROM gp_stat_all_tables WHERE relname = 'vacuum_progress_ao_row' and gp_segment_id = 1;

-- Perform VACUUM again to recycle the remaining awaiting drop segment marked by the previous run.
SELECT gp_inject_fault('vacuum_ao_after_index_delete', 'suspend', dbid) FROM gp_segment_configuration WHERE content = 0 AND role = 'p';
SELECT gp_inject_fault('appendonly_after_truncate_segment_file', 'suspend', dbid) FROM gp_segment_configuration WHERE content > 0 AND role = 'p';
1&: VACUUM vacuum_progress_ao_row;
-- Resume execution and entering pre_cleanup phase, suspend at vacuuming indexes for segment 0.
SELECT gp_wait_until_triggered_fault('vacuum_ao_after_index_delete', 1, dbid) FROM gp_segment_configuration WHERE content = 0 AND role = 'p';
-- Resume execution and moving on to truncate segments that were marked as AWAITING_DROP for segment 1 and 2, there should be only 1.
SELECT gp_wait_until_triggered_fault('appendonly_after_truncate_segment_file', 1, dbid) FROM gp_segment_configuration WHERE content > 0 AND role = 'p';
-- Segment 0 is in vacuuming indexes phase (part of ao pre_cleanup phase), index_vacuum_count should increase to 1.
select relid::regclass as relname, phase, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count, max_dead_tuples, num_dead_tuples from gp_stat_progress_vacuum where gp_segment_id = 0;
-- Segment 1 and 2 are in truncate segments phase (part of ao post_cleanup phase), heap_blks_vacuumed should increase to 1.
select relid::regclass as relname, phase, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count, max_dead_tuples, num_dead_tuples from gp_stat_progress_vacuum where gp_segment_id > 0;
select relid::regclass as relname, phase, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count, max_dead_tuples, num_dead_tuples from gp_stat_progress_vacuum_summary;

SELECT gp_inject_fault('appendonly_after_truncate_segment_file', 'reset', dbid) FROM gp_segment_configuration WHERE content > -1 AND role = 'p';
SELECT gp_inject_fault('vacuum_ao_after_index_delete', 'reset', dbid) FROM gp_segment_configuration WHERE content = 0 AND role = 'p';
1<:

-- Vacuum has finished, nothing should show up in the view.
select relid::regclass as relname, phase, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count, max_dead_tuples, num_dead_tuples from gp_stat_progress_vacuum where gp_segment_id = 1;
select relid::regclass as relname, phase, heap_blks_total, heap_blks_scanned, heap_blks_vacuumed, index_vacuum_count, max_dead_tuples, num_dead_tuples from gp_stat_progress_vacuum_summary;

-- pg_class and collected stats view should be updated after the 2nd VACUUM
1U: SELECT wait_until_vacuum_count_change_to('vacuum_progress_ao_row'::regclass::oid, 2);
SELECT relpages, reltuples, relallvisible FROM pg_class where relname = 'vacuum_progress_ao_row';
SELECT n_live_tup, n_dead_tup, last_vacuum is not null as has_last_vacuum, vacuum_count FROM gp_stat_all_tables WHERE relname = 'vacuum_progress_ao_row' and gp_segment_id = 1;
SELECT n_live_tup, n_dead_tup, last_vacuum is not null as has_last_vacuum, vacuum_count FROM gp_stat_all_tables_summary WHERE relname = 'vacuum_progress_ao_row';

-- Cleanup
SELECT gp_inject_fault_infinite('all', 'reset', dbid) FROM gp_segment_configuration;
reset Debug_appendonly_print_compaction;
reset default_table_access_method;
