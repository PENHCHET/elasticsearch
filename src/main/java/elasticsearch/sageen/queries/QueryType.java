/**
 * 
 */
package elasticsearch.sageen.queries;



public enum QueryType {

	TERM, 
	TERMS,
	MATCH, 
	MATCH_PHRASE,
	WILDCARD, 
	SEARCH_BY_ID, 
	GET_SINGLE_FIELD,
	BOOL_QUERY,
	BOOST,
	RANGE_QUERY,
	MATCH_ALL,
	QUERY_STRING_QUERY,
	FUZZY,
	PREFIX
}
