package main

import (
	"bytes"
	"fmt"
	"log"
	"os"

	"strconv"

	"strings"

	. "github.com/xwb1989/sqlparser"
	"gopkg.in/mgo.v2/bson"
)

var comparisonOperatorMap = map[string]string{
	AST_EQ: "$eq",
	AST_LT: "$lt",
	AST_GT: "$gt",
	AST_LE: "$lte",
	AST_GE: "$gte",
	AST_NE: "$ne",
	//AST_NSE      : "<=>",
	AST_IN:     "$in",
	AST_NOT_IN: "$nin",
	//AST_LIKE     : "like",
	//AST_NOT_LIKE : "not like",
}

func convertPlainExpr(val ValExpr) interface{} {
	var res interface{}

	switch r := val.(type) {
	case NumVal:
		var err error
		res, err = strconv.Atoi(string(r))
		if err != nil {
			res, _ = strconv.ParseFloat(string(r), 64)
		}
	case StrVal:
		res = string(r)
	case ValTuple:
		res = convertExpr(r)
	case nil:
		res = nil
	default:
		log.Fatalf("Value must be num or str, got %T", val)
	}

	return res
}

func col(e *ColName) string {
	var res = string(e.Name)
	if e.Qualifier != nil {
		res = string(e.Qualifier) + "." + res
	}
	return strings.Replace(res, "__", ".", -1)
}

func convertExpr(rawE Expr) interface{} {
	ret := make(bson.M)

	switch e := rawE.(type) {
	case *AndExpr:
		ret["$and"] = []interface{}{convertExpr(e.Left), convertExpr(e.Right)}
	case *OrExpr:
		ret["$or"] = []interface{}{convertExpr(e.Left), convertExpr(e.Right)}
	case *NotExpr:
		ret["$not"] = convertExpr(e.Expr)
	case *ParenBoolExpr:
		return convertExpr(e.Expr)
	case *ComparisonExpr:
		k, ok := comparisonOperatorMap[e.Operator]
		if !ok {
			log.Fatalf("Operator %s cannot be mapped to mongodb query", e.Operator)
		}

		ret[col(e.Left.(*ColName))] = bson.M{k: convertPlainExpr(e.Right)}
	case *RangeCond:
		var f = col(e.Left.(*ColName))

		ret["$and"] = []interface{}{
			bson.M{f: bson.M{"$gte": convertPlainExpr(e.From)}},
			bson.M{f: bson.M{"$lte": convertPlainExpr(e.To)}},
		}

		if e.Operator == AST_NOT_BETWEEN {
			ret = bson.M{"$not": ret}
		}
	case *NullCheck:
		what := "$eq"
		if e.Operator == AST_IS_NOT_NULL {
			what = "$neq"
		}

		ret[col(e.Expr.(*ColName))] = bson.M{what: nil}
	case *ExistsExpr:
		log.Fatal("EXISTS is not supported")
	case NumVal:
		return string(e)
	case StrVal:
		return string(e)
	case ValArg:
		log.Fatal("Binding of arguments is not supported")
	case *NullVal:
		return nil
	case *ColName:
		return col(e)
	case ValTuple:
		var res = make([]interface{}, 0, len(e))
		for _, el := range e {
			res = append(res, convertExpr(el))
		}
		return res
	case *Subquery:
		log.Fatal("Subqueries are not supported")
	case ListArg:
		log.Fatal("List args are not supported")
	case *BinaryExpr:
		log.Fatalf("Binary expressions, including '%c' are not supported", e.Operator)
	case *UnaryExpr:
		log.Fatalf("Unary expressions, including '%c' are not supported", e.Operator)
	case *FuncExpr:
		log.Fatalf("Func expressions, including '%s' are not supported", e.Name)
	case *CaseExpr:
		log.Fatal("Case expressions are not supported")
	}

	return ret
}

func formatJSON(in interface{}) string {
	out, err := bson.MarshalJSON(in)
	if err != nil {
		log.Fatalf("Could not marshal json: %s", err.Error())
	}
	return string(bytes.TrimSpace(out))
}

func main() {
	if len(os.Args) != 2 {
		log.Fatalf("Usage: %s '<query>'", os.Args[0])
	}

	query := os.Args[1]

	res_, err := Parse(query)
	if err != nil {
		log.Fatalf("Could not parse sql: %s", err.Error())
	}

	res := res_.(*Select)

	//pretty.Log(res)
	//fmt.Fprint(os.Stderr, "\n\n")

	var mongoQuery interface{}
	if res.Where != nil {
		mongoQuery = convertExpr(res.Where.Expr)
	}
	var collectionName string

	if len(res.From) > 1 {
		log.Fatalf("Can only select from a single table at the moment (provided %d)", len(res.From))
	}

	collectionName = string(res.From[0].(*AliasedTableExpr).Expr.(*TableName).Name)

	method := "find"
	projectionMap := make(bson.M)

	for _, expr := range res.SelectExprs {
		switch e := expr.(type) {
		case *StarExpr:
			if len(res.SelectExprs) == 1 && e.TableName == nil {
				break
			}

			log.Fatalf("Star expressions other than '*' (%s) are not supported at the moment", e)
		case *NonStarExpr:
			if e.As != nil {
				log.Fatalf("AS (%s) is not supported at the moment in select queries", e)
			}

			switch f:= e.Expr.(type) {
			case *ColName:
				projectionMap[col(f)] = true
			case *FuncExpr:
				f.Name = bytes.ToLower(f.Name)

				if !bytes.Equal(f.Name, []byte("count")) {
					log.Fatalf("Unsupported function is SELECT fields: %s, only count is supported", f.Name)
				}

				if len(res.SelectExprs) != 1 {
					log.Fatal("'count' must be the only function is SELECT fields at the moment")
				}

				if len(f.Exprs) != 1 {
					log.Fatal("'count' must have only a single argument")
				}

				if f.Distinct {
					log.Fatal("'count(distinct ...)' is not supported")
				}

				switch fe := f.Exprs[0].(type) {
				case *StarExpr:
				case *NonStarExpr:
					st, ok := fe.Expr.(*ColName)
					if !ok {
						log.Fatal("Only count(*) and count(colname) are supported")
					}

					mongoQuery = bson.M{
						"$and": []interface{}{
							bson.M{col(st): bson.M{"$exists": true}},
							mongoQuery,
						},
					}
				}

				method = "count"
			default:
				log.Fatalf("Supported %T in SELECT fields", f)
			}
		default:
			log.Fatalf("Internal inconsistency, got type %T", e)
		}
	}

	projectionArg := ""
	if len(projectionMap) > 0 {
		projectionArg = ", " + formatJSON(projectionMap)
	}

	parts := []string{
		"db",
		collectionName,
		fmt.Sprintf("%s(%s%s)", method, formatJSON(mongoQuery), projectionArg),
	}

	if res.Limit != nil {
		rowC, _ := convertPlainExpr(res.Limit.Rowcount).(int)
		if rowC > 0 {
			parts = append(parts, fmt.Sprintf("limit(%d)", rowC))
		}

		skip, _ := convertPlainExpr(res.Limit.Offset).(int)
		if skip > 0 {
			parts = append(parts, fmt.Sprintf("skip(%d)", skip))
		}
	}

	fmt.Printf("%s", strings.Join(parts, "."))
}
