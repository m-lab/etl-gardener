package bq

import "html/template"

var DedupQuery = dedupQuery

// JoinQuery returns the appropriate query in string form.
func JoinQuery(to TableOps, dt string) string {
	switch dt {
	case "annotation":
		return to.makeQuery(joinAnnotationTemplate)
	case "hopannotation1":
		return to.makeQuery(joinHopsTemplate)
	default:
		return to.makeQuery(template.New(""))
	}
}
