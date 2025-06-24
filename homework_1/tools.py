"""
Simple tools to help the lineage agent parse code more accurately
"""

import re


class CodeAnalysisTools:
    """Simple tools for analyzing code structure"""

    @staticmethod
    def extract_table_sources(code: str) -> list[str]:
        """Extract table/file sources from code"""
        sources = []

        # Pandas file operations
        pandas_patterns = [
            r'pd\.read_csv\s*\(\s*["\']([^"\']+)["\']',
            r'pd\.read_parquet\s*\(\s*["\']([^"\']+)["\']',
            r'pd\.read_json\s*\(\s*["\']([^"\']+)["\']',
            r'pd\.read_excel\s*\(\s*["\']([^"\']+)["\']',
        ]

        # Spark table operations
        spark_patterns = [
            r'spark\.table\s*\(\s*["\']([^"\']+)["\']',
            r'spark\.read\.[^(]+\s*\(\s*["\']([^"\']+)["\']',
        ]

        all_patterns = pandas_patterns + spark_patterns

        for pattern in all_patterns:
            matches = re.findall(pattern, code)
            sources.extend(matches)

        return list(set(sources))  # Remove duplicates

    @staticmethod
    def extract_variable_assignments(code: str) -> dict[str, list[int]]:
        """Extract all variable assignments and their line numbers"""
        assignments = {}
        lines = code.split("\n")

        for i, line in enumerate(lines, 1):
            # Simple regex to find assignments
            assignment_match = re.search(r"^(\w+)\s*=", line.strip())
            if assignment_match:
                var_name = assignment_match.group(1)
                if var_name not in assignments:
                    assignments[var_name] = []
                assignments[var_name].append(i)

        return assignments

    @staticmethod
    def extract_operations(code: str) -> list[dict]:
        """Extract operations from code with detailed aggregation detection"""
        operations = []
        lines = code.split("\n")

        # Enhanced operation patterns with specific aggregations
        operation_patterns = {
            # Pandas operations
            "filter": [r"\.query\s*\(", r"\[.*[<>=].*\]"],
            "groupby": [r"\.groupby\s*\("],
            "merge": [r"\.merge\s*\("],
            "join": [r"\.join\s*\("],
            "pivot_table": [r"\.pivot_table\s*\("],
            "drop": [r"\.drop\s*\(", r"\.dropna\s*\("],
            "fill": [r"\.fillna\s*\("],
            "sort": [r"\.sort_values\s*\(", r"\.orderBy\s*\("],
            "select": [r"\.select\s*\(", r"\[\[.*\]\]"],
            "reset_index": [r"\.reset_index\s*\("],
            "set_index": [r"\.set_index\s*\("],
            # Specific pandas aggregations
            "agg_sum": [r"\.sum\s*\(", r'["\']sum["\']', r"agg.*sum"],
            "agg_mean": [r"\.mean\s*\(", r'["\']mean["\']', r"agg.*mean"],
            "agg_count": [r"\.count\s*\(", r'["\']count["\']', r"agg.*count"],
            "agg_max": [r"\.max\s*\(", r'["\']max["\']', r"agg.*max"],
            "agg_min": [r"\.min\s*\(", r'["\']min["\']', r"agg.*min"],
            "agg_std": [r"\.std\s*\(", r'["\']std["\']'],
            "agg_median": [r"\.median\s*\(", r'["\']median["\']'],
            "agg_nunique": [r"\.nunique\s*\(", r'["\']nunique["\']'],
            # Conversion operations
            "to_dict": [r"\.to_dict\s*\("],
            "to_pandas": [r"\.toPandas\s*\("],
            "to_csv": [r"\.to_csv\s*\("],
            "to_parquet": [r"\.to_parquet\s*\("],
            # Spark operations
            "filter_spark": [r"\.filter\s*\("],
            "groupBy": [r"\.groupBy\s*\("],
            "withColumn": [r"\.withColumn\s*\("],
            "alias": [r"\.alias\s*\("],
            # Specific Spark aggregations
            "agg_sum_spark": [r"F\.sum\s*\(", r'\.sum\("'],
            "agg_avg_spark": [r"F\.avg\s*\(", r'\.avg\("'],
            "agg_count_spark": [r"F\.count\s*\(", r'\.count\("'],
            "agg_max_spark": [r"F\.max\s*\(", r'\.max\("'],
            "agg_min_spark": [r"F\.min\s*\(", r'\.min\("'],
            "agg_stddev_spark": [r"F\.stddev\s*\("],
            "agg_countDistinct_spark": [r"F\.countDistinct\s*\("],
            # Window functions
            "window_row_number": [r"F\.row_number\s*\(", r"ROW_NUMBER\s*\("],
            "window_rank": [r"F\.rank\s*\(", r"RANK\s*\("],
            "window_percent_rank": [r"F\.percent_rank\s*\("],
            # SQL aggregations
            "sql_SUM": [r"\bSUM\s*\("],
            "sql_AVG": [r"\bAVG\s*\("],
            "sql_COUNT": [r"\bCOUNT\s*\("],
            "sql_MAX": [r"\bMAX\s*\("],
            "sql_MIN": [r"\bMIN\s*\("],
            "sql_STDDEV": [r"\bSTDDEV\s*\("],
            "sql_VARIANCE": [r"\bVARIANCE\s*\("],
            "sql_PERCENTILE": [r"\bPERCENTILE_APPROX\s*\("],
        }

        for i, line in enumerate(lines, 1):
            for op_type, patterns in operation_patterns.items():
                for pattern in patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        operations.append(
                            {"line": i, "operation": op_type, "code": line.strip()}
                        )
                        break

        return operations

    @staticmethod
    def trace_variable_dependencies(code: str, target_variable: str) -> dict:
        """Trace dependencies for a specific variable"""
        assignments = CodeAnalysisTools.extract_variable_assignments(code)
        operations = CodeAnalysisTools.extract_operations(code)
        sources = CodeAnalysisTools.extract_table_sources(code)

        # Find lines where target variable is defined
        target_lines = assignments.get(target_variable, [])

        # Find relevant operations (simplified)
        relevant_ops = []
        if target_lines:
            target_line = target_lines[-1]  # Last assignment
            # Get operations that might be relevant (basic heuristic)
            for op in operations:
                if op["line"] <= target_line:
                    relevant_ops.append(op["operation"])

        return {
            "target_variable": target_variable,
            "sources": sources,
            "operations": list(set(relevant_ops)),  # Remove duplicates
            "assignment_lines": target_lines,
            "total_operations": len(operations),
        }
