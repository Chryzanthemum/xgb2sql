import json
from typing import List


def xgb2sql(xgb_booster, table_name: str, index_list=[], sql_type=None):
    """
    Takes in an XGB Booster and converts it to a SQL query.
    Look, I'm not saying you should use this, but I'm saying it now exists.
    I imagine any sort of tree based model could be relatively easily converted to a SQL query using this.

    Parameters
    ----------
    xgb_booster: xgboost.core.Booster
        https://xgboost.readthedocs.io/en/latest/tutorials/model.html
    table_name: str
        The name of the SQL table to query from. Obviously this table must be the same as the model inputs or else it won't work.
    index_list : list
        Anything in the list will be passed through as a column in your final output.
    sql_type : str
        If there's a better way native to the sql_type to generate the code, it will be used.
        Otherwise defaults to PostgreSQL compliant code.
    """

    def _json_parse(xgb_booster) -> str:

        ret = xgb_booster.get_dump(dump_format="json")

        json_string = "[\n"
        for i, _ in enumerate(ret):
            json_string = json_string + ret[i]
            if i < len(ret) - 1:
                json_string = json_string + ",\n"
        json_string = json_string + "\n]"

        return json.loads(json_string)

    def _psql_eval(index_list: List[str], leaf_list: List) -> str:

        column_string = "\n\t+ ".join(columns)
        if len(index_list) > 0:
            query = f"""\nSELECT
    {index_string},
    1 / ( 1 + EXP ( - (
    {column_string}) ) ) AS score
FROM booster_output"""
        else:
            query = f"""\nSELECT
    1 / ( 1 + EXP ( - (
    {column_string} ) ) ) AS score
FROM booster_output"""

        return query

    def _bq_eval(index_list: List[str]) -> str:
        def _string_parse(index_list: List[str]) -> str:

            a = ["'" + i + "'" for i in index_list]
            return ",".join(a)

        if len(index_list) > 0:
            query = f""",

json_collapsed AS (
    SELECT
        {index_string},
        TO_JSON_STRING(booster_output) AS json_text
    FROM booster_output
),

unnested AS (
    SELECT
        {index_string},
        REGEXP_REPLACE(SUBSTR(pairs, 1, STRPOS(pairs, ':') - 1), '^"|"$', '') AS variable_name,
        REGEXP_REPLACE(SUBSTR(pairs, STRPOS(pairs, ':') + 1, LENGTH(pairs)), '^"|"$', '') AS value
    FROM json_collapsed, UNNEST(SPLIT(REGEXP_REPLACE(json_text, '^{{|}}$', ''), ',"')) pairs
)

SELECT
    {index_string},
    1 / ( 1 + EXP ( - SUM ( CAST ( value AS FLOAT64 ) ) ) ) AS score,
FROM unnested
WHERE variable_name NOT IN (
    {_string_parse(index_list)}
)
GROUP BY {index_string}
"""

        else:
            query = f""",

json_collapsed AS (
    SELECT
        TO_JSON_STRING(branching) AS json_text
    FROM booster_output
),

unnested AS (
    SELECT
        REGEXP_REPLACE(SUBSTR(pairs, 1, STRPOS(pairs, ':') - 1), '^"|"$', '') AS variable_name,
        REGEXP_REPLACE(SUBSTR(pairs, STRPOS(pairs, ':') + 1, LENGTH(pairs)), '^"|"$', '') AS value
    FROM json_collapsed, UNNEST(SPLIT(REGEXP_REPLACE(json_text, '^{{|}}$', ''), ',"')) pairs
)

SELECT
    1 / ( 1 + EXP ( - SUM ( CAST ( value AS FLOAT64 ) ) ) ) AS score
FROM unnested
"""
        return query

    def _extract_values(obj, key):

        key_dict = {}
        arr = []
        info_dict = {}

        def _extract(obj, arr, key, prev=None):

            if isinstance(obj, dict):
            # If every row is filled out for a column, then obj["missing"] is never there.
                node_info = {"parent": prev}
    
                if "split" in obj:
                    node_info.update({
                        "split_column": obj["split"],
                        "if_less_than": obj.get("yes"),
                        "if_greater_than": obj.get("no"),
                    })
    
                    if "split_condition" in obj:
                        node_info["split_number"] = obj["split_condition"]
                    
                    if "missing" in obj:
                        node_info["if_null"] = obj["missing"]
    
                info_dict[obj["nodeid"]] = node_info
    
                prev = obj["nodeid"]
    
                for k, v in obj.items():
                    if isinstance(v, (dict, list)):
                        _extract(v, arr, key, prev)
                    elif k == key:
                        key_dict.update({obj["nodeid"]: v})
            elif isinstance(obj, list):
                for item in obj:
                    _extract(item, arr, key, prev)
            return key_dict

        results = _extract(obj, arr, key)
        return results, info_dict

    def _recurse_backwards(first_node) -> str:

        query_list: List[str] = []
    
        def _recurse(x) -> None:
    
            prev_node = x
            next_node = splits[prev_node]["parent"]
            try:
                node = splits[next_node]
                if (node["if_less_than"] == prev_node) and (
                    "if_null" in node
                ):
                    text = f"(({node['split_column']} < {node['split_number']}) OR ({node['split_column']} IS NULL))"
                    query_list.insert(0, text)
                    _recurse(next_node)
                elif node["if_less_than"] == prev_node and "split_number" in node:
                    text = f"({node['split_column']} < {node['split_number']})"
                    query_list.insert(0, text)
                    _recurse(next_node)
                elif node["if_less_than"] == prev_node:
                    text = f"({node['split_column']} < 1)"
                    query_list.insert(0, text)
                    _recurse(next_node)
                elif (node["if_greater_than"] == prev_node) & (
                    "if_null" in node
                ):
                    text = f"(({node['split_column']} >= {node['split_number']}) OR ({node['split_column']} IS NULL))"
                    query_list.insert(0, text)
                    _recurse(next_node)
                elif node["if_greater_than"] == prev_node and "split_number" in node:
                    text = f"({node['split_column']} >= {node['split_number']})"
                    query_list.insert(0, text)
                    _recurse(next_node)
                elif node["if_greater_than"] == prev_node:
                    text = f"({node['split_column']} >= 1)"
                    query_list.insert(0, text)
                    _recurse(next_node)
            except:
                pass

    _recurse(first_node)

    s = "\n\t\t\tAND "

    return s.join(query_list)

    tree_json = _json_parse(xgb_booster)

    index_list = [str(i) for i in index_list]
    index_string = ",\n".join(index_list)

    leaf_list = []
    columns = []
    counter = 0
    for i in range(0, len(tree_json)):
        leaves, splits = _extract_values(tree_json[i], "leaf")
        when_list = []
        column = f"column_{counter}"
        columns.append(column)
        if len(leaves) == 1:
            column_list = f"{leaves.values()[0]} AS {column}"
        else:
            for base_leaf in leaves:
                leaf_query = (
                    "\t\t\tWHEN "
                    + _recurse_backwards(base_leaf)
                    + f"\n\t\tTHEN {leaves[base_leaf]}"
                )

                when_list.append(leaf_query)


            column_list = "\t\tCASE\n" + ("\n").join(when_list) + f"\n\t\tEND AS {column}"

        leaf_list.append(column_list)
        counter += 1

    if sql_type == "bigquery":
        output = _bq_eval(index_list)
    else:
        output = _psql_eval(index_list, leaf_list)

    query = (
        "WITH booster_output AS (\n\tSELECT\n"
        + ", \n".join((index_list + leaf_list))
        + f"\n\tFROM {table_name}"
        + f"\n{output}"
    )

    return query
