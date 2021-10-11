package bq

var DedupQuery = dedupQuery

// JoinQuery returns the appropriate query in string form.
func JoinQuery(to TableOps) string {
	return to.makeQuery(joinTemplate)
}
