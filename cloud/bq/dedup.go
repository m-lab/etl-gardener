package bq

import (
	"flag"
)

// Use with caution!
func isTest() bool {
	return flag.Lookup("test.v") != nil
}

// NOTE: the following dedup templates are preserved for reference but are no longer used.
// They should ultimately be deleted once the v1 datatypes are migrated to the v2 pipeline.
// TODO(github.com/m-lab/etl/issues/1096)

// as mlab-oti:batch.ndt_20170601.
//
// Some tests are collected as both uncompressed and compressed files. In some historical
// archives (June 2017), files for a single test appear in different tar files, which
// results in duplicate rows.
// This query strips the gz, finds duplicates, and chooses the best row -  prefering gzipped
// files, and prefering later parse_time.
var dedupTemplateNDT = `
	#standardSQL
	# Delete all duplicate rows based on test_id, preferring gz over non-gz, later parse_time
	SELECT * except (row_number, gz, stripped_id)
    from (
		select *,
		# Prefer more snapshots, metadata, earlier task names, gzipped, later parse time
		ROW_NUMBER() OVER (PARTITION BY stripped_id ORDER BY anomalies.num_snaps DESC, anomalies.no_meta, task_filename, gz DESC, parse_time DESC) row_number
        FROM (
			SELECT *,
			    REGEXP_REPLACE(test_id, ".gz$", "") AS stripped_id,
		        REGEXP_EXTRACT(test_id, ".*(.gz)$") AS gz
	        FROM ` + "`%s`" + `
        )
    )
	WHERE row_number = 1`

// TODO - add selection by latest parse_time.
var dedupTemplateSidestream = `
	#standardSQL
	# Select single row based on test_id, 5-tuple, start-time
	SELECT * EXCEPT (row_number)
    FROM ( SELECT *, ROW_NUMBER() OVER (
        PARTITION BY CONCAT(test_id, cast(web100_log_entry.snap.StartTimeStamp as string),
            web100_log_entry.connection_spec.local_ip, cast(web100_log_entry.connection_spec.local_port as string),
			web100_log_entry.connection_spec.remote_ip, cast(web100_log_entry.connection_spec.remote_port as string))
		# Use the most recently parsed row
		ORDER BY parse_time DESC
		) row_number
	    FROM ` + "`%s`" + `)
	WHERE row_number = 1`

// dedupTemplateTraceroute expects to be executed on a table containing a single day's data, such
// as mlab-oti:batch.traceroute_20170601.
var dedupTemplateTraceroute = `
	#standardSQL
	# Select single row based on TestTime, client_ip, server_ip
	SELECT * EXCEPT (row_number)
    FROM (
		SELECT *, ROW_NUMBER() OVER (
			PARTITION BY CONCAT(STRING(TestTime), Source.IP, DESTINATION.IP)
			# Prefer later parse time
			ORDER BY ParseInfo.ParseTime DESC
		) row_number
	    FROM ` + "`%s`" + `)
	WHERE row_number = 1`
