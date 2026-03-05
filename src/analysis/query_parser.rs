use sqlparser::ast::{
    BinaryOperator, Expr, FromTable, Join, JoinConstraint, OrderByExpr, Query, Select, SelectItem,
    SetExpr, Statement, TableFactor, TableWithJoins,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::{Parser, ParserError};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct TableRef {
    pub schema: Option<String>,
    pub name: String,
}

impl TableRef {
    pub fn full_name(&self) -> String {
        match &self.schema {
            Some(schema) => format!("{}.{}", schema, self.name),
            None => self.name.clone(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TableColumnUsage {
    pub equality_filters: Vec<String>,
    pub non_equality_filters: Vec<String>,
    pub equality_joins: Vec<String>,
    pub orders: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct QueryColumnUsage {
    pub tables: Vec<TableRef>,
    pub usage_by_table: HashMap<String, TableColumnUsage>,
}

#[derive(Debug)]
struct PendingColumn {
    relation: Option<String>,
    name: String,
    kind: ColumnKind,
}

#[derive(Debug, Clone, Copy)]
enum ColumnKind {
    EqualityFilter,
    NonEqualityFilter,
    EqualityJoin,
    Order,
}

pub fn parse_query_columns(query: &str) -> Result<QueryColumnUsage, ParserError> {
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, query)?;

    let statement = statements
        .pop()
        .ok_or_else(|| ParserError::ParserError("empty query".into()))?;

    let mut collector = QueryColumnCollector::default();
    collector.collect_statement(&statement)?;

    Ok(collector.into_usage())
}

#[derive(Default)]
struct QueryColumnCollector {
    tables: Vec<TableRef>,
    alias_map: HashMap<String, String>,
    pending: Vec<PendingColumn>,
    resolved_usage_by_table: HashMap<String, TableColumnUsage>,
}

impl QueryColumnCollector {
    fn collect_statement(&mut self, statement: &Statement) -> Result<(), ParserError> {
        match statement {
            Statement::Query(query) => self.collect_query(query),
            Statement::Update {
                table,
                from,
                selection,
                ..
            } => self.collect_update(table, from, selection),
            Statement::Delete {
                from,
                using,
                selection,
                order_by,
                ..
            } => self.collect_delete(from, using, selection, order_by),
            _ => return Err(ParserError::ParserError("unsupported statement".into())),
        }
        Ok(())
    }

    fn collect_query(&mut self, query: &Query) {
        self.collect_set_expr(&query.body);

        for order in &query.order_by {
            self.collect_order_by(order);
        }
    }

    fn collect_update(
        &mut self,
        table: &TableWithJoins,
        from: &Option<TableWithJoins>,
        selection: &Option<Expr>,
    ) {
        self.collect_table_with_joins(table);

        if let Some(from_table) = from {
            self.collect_table_with_joins(from_table);
        }

        if let Some(filter) = selection {
            self.collect_filter_expr(filter);
        }
    }

    fn collect_delete(
        &mut self,
        from: &FromTable,
        using: &Option<Vec<TableWithJoins>>,
        selection: &Option<Expr>,
        order_by: &[OrderByExpr],
    ) {
        let from_tables = match from {
            FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => tables,
        };
        for table in from_tables {
            self.collect_table_with_joins(table);
        }

        if let Some(using_tables) = using {
            for table in using_tables {
                self.collect_table_with_joins(table);
            }
        }

        if let Some(filter) = selection {
            self.collect_filter_expr(filter);
        }

        for order in order_by {
            self.collect_order_by(order);
        }
    }

    fn collect_set_expr(&mut self, set_expr: &SetExpr) {
        match set_expr {
            SetExpr::Select(select) => self.collect_select(select),
            SetExpr::Query(query) => self.collect_query(query.as_ref()),
            SetExpr::SetOperation { left, right, .. } => {
                self.collect_set_expr(left.as_ref());
                self.collect_set_expr(right.as_ref());
            }
            _ => {}
        }
    }

    fn collect_select(&mut self, select: &Select) {
        for table in &select.from {
            self.collect_table_with_joins(table);
        }

        if let Some(selection) = &select.selection {
            self.collect_filter_expr(selection);
        }

        for item in &select.projection {
            if let SelectItem::UnnamedExpr(expr) = item {
                self.collect_projection_expr(expr);
            }
        }
    }

    fn collect_table_with_joins(&mut self, table: &TableWithJoins) {
        let mut left_tables = Vec::new();
        if let Some(table_name) = self.collect_table_factor(&table.relation) {
            left_tables.push(table_name);
        }

        for join in &table.joins {
            let right_table = self.collect_join(join, &left_tables);
            if let Some(table_name) = right_table {
                left_tables.push(table_name);
            }
        }
    }

    fn collect_table_factor(&mut self, table_factor: &TableFactor) -> Option<String> {
        match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let (schema, table) = parse_object_name(name);
                let table_ref = TableRef {
                    schema: schema.clone(),
                    name: table.clone(),
                };
                let full_name = table_ref.full_name();
                self.tables.push(table_ref);
                self.alias_map.insert(table.clone(), full_name.clone());
                if let Some(schema_name) = schema {
                    self.alias_map
                        .insert(format!("{}.{}", schema_name, table), full_name.clone());
                }
                if let Some(alias) = alias {
                    self.alias_map
                        .insert(normalize_ident(&alias.name), full_name.clone());
                }
                Some(full_name)
            }
            TableFactor::Derived { subquery, .. } => {
                let mut nested = QueryColumnCollector::default();
                nested.collect_query(subquery.as_ref());
                self.merge_usage(nested.into_usage());
                None
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.collect_table_with_joins(table_with_joins.as_ref());
                None
            }
            _ => None,
        }
    }

    fn collect_join(&mut self, join: &Join, left_tables: &[String]) -> Option<String> {
        let right_table = self.collect_table_factor(&join.relation);

        match &join.join_operator {
            sqlparser::ast::JoinOperator::Inner(constraint)
            | sqlparser::ast::JoinOperator::LeftOuter(constraint)
            | sqlparser::ast::JoinOperator::RightOuter(constraint)
            | sqlparser::ast::JoinOperator::FullOuter(constraint) => match constraint {
                JoinConstraint::On(expr) => self.collect_join_expr(expr),
                JoinConstraint::Using(columns) => {
                    for column in columns {
                        for table in left_tables {
                            self.pending.push(PendingColumn {
                                relation: Some(table.clone()),
                                name: normalize_ident(column),
                                kind: ColumnKind::EqualityJoin,
                            });
                        }
                        if let Some(table) = &right_table {
                            self.pending.push(PendingColumn {
                                relation: Some(table.clone()),
                                name: normalize_ident(column),
                                kind: ColumnKind::EqualityJoin,
                            });
                        }
                    }
                }
                _ => {}
            },
            _ => {}
        }

        right_table
    }

    fn collect_filter_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And | BinaryOperator::Or => {
                    self.collect_filter_expr(left);
                    self.collect_filter_expr(right);
                }
                BinaryOperator::Eq => {
                    self.collect_equality_predicate(left, right, ColumnKind::EqualityFilter);
                }
                _ => self.collect_non_equality_predicate(left, right),
            },
            Expr::InList { expr, .. } => {
                self.push_column_if_applicable(expr, ColumnKind::NonEqualityFilter)
            }
            Expr::Between { expr, .. } => {
                self.push_column_if_applicable(expr, ColumnKind::NonEqualityFilter)
            }
            Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
                self.push_column_if_applicable(expr, ColumnKind::NonEqualityFilter)
            }
            Expr::Nested(expr) => self.collect_filter_expr(expr),
            _ => {}
        }
    }

    fn collect_join_expr(&mut self, expr: &Expr) {
        match expr {
            Expr::BinaryOp { left, op, right } => match op {
                BinaryOperator::And | BinaryOperator::Or => {
                    self.collect_join_expr(left);
                    self.collect_join_expr(right);
                }
                BinaryOperator::Eq => {
                    self.collect_equality_predicate(left, right, ColumnKind::EqualityJoin);
                }
                _ => {}
            },
            Expr::Nested(expr) => self.collect_join_expr(expr),
            _ => {}
        }
    }

    fn collect_order_by(&mut self, order: &OrderByExpr) {
        self.push_column_if_applicable(&order.expr, ColumnKind::Order);
    }

    fn collect_projection_expr(&mut self, expr: &Expr) {
        if let Expr::Nested(expr) = expr {
            self.collect_projection_expr(expr);
        }
    }

    fn push_column_if_applicable(&mut self, expr: &Expr, kind: ColumnKind) {
        if let Some(column) = column_ref_from_expr(expr) {
            self.pending.push(PendingColumn {
                relation: column.relation,
                name: column.name,
                kind,
            });
        }
    }

    fn collect_equality_predicate(&mut self, left: &Expr, right: &Expr, default_kind: ColumnKind) {
        match (column_ref_from_expr(left), column_ref_from_expr(right)) {
            (Some(left_column), Some(right_column)) => {
                self.pending.push(PendingColumn {
                    relation: left_column.relation,
                    name: left_column.name,
                    kind: ColumnKind::EqualityJoin,
                });
                self.pending.push(PendingColumn {
                    relation: right_column.relation,
                    name: right_column.name,
                    kind: ColumnKind::EqualityJoin,
                });
            }
            _ => {
                self.push_column_if_applicable(left, default_kind);
                self.push_column_if_applicable(right, default_kind);
            }
        }
    }

    fn collect_non_equality_predicate(&mut self, left: &Expr, right: &Expr) {
        self.push_column_if_applicable(left, ColumnKind::NonEqualityFilter);
        self.push_column_if_applicable(right, ColumnKind::NonEqualityFilter);
    }

    fn merge_usage(&mut self, usage: QueryColumnUsage) {
        for table in usage.tables {
            if !self
                .tables
                .iter()
                .any(|existing| existing.full_name() == table.full_name())
            {
                self.tables.push(table);
            }
        }

        for (table_name, table_usage) in usage.usage_by_table {
            let entry = self.resolved_usage_by_table.entry(table_name).or_default();
            merge_table_usage(entry, &table_usage);
        }
    }

    fn into_usage(self) -> QueryColumnUsage {
        let QueryColumnCollector {
            tables,
            alias_map,
            pending,
            mut resolved_usage_by_table,
        } = self;

        let default_table = if tables.len() == 1 {
            Some(tables[0].full_name())
        } else {
            None
        };

        for pending in pending {
            let table = resolve_table_name(pending.relation.as_deref(), &alias_map, &default_table);
            let Some(table_name) = table else { continue };
            let entry = resolved_usage_by_table.entry(table_name).or_default();
            match pending.kind {
                ColumnKind::EqualityFilter => {
                    push_unique(&mut entry.equality_filters, &pending.name)
                }
                ColumnKind::NonEqualityFilter => {
                    push_unique(&mut entry.non_equality_filters, &pending.name)
                }
                ColumnKind::EqualityJoin => push_unique(&mut entry.equality_joins, &pending.name),
                ColumnKind::Order => push_unique(&mut entry.orders, &pending.name),
            }
        }

        QueryColumnUsage {
            tables,
            usage_by_table: resolved_usage_by_table,
        }
    }
}

#[derive(Debug)]
struct ColumnRef {
    relation: Option<String>,
    name: String,
}

fn column_ref_from_expr(expr: &Expr) -> Option<ColumnRef> {
    match expr {
        Expr::Identifier(ident) => Some(ColumnRef {
            relation: None,
            name: normalize_ident(ident),
        }),
        Expr::CompoundIdentifier(idents) => {
            if idents.len() == 2 {
                Some(ColumnRef {
                    relation: Some(normalize_ident(&idents[0])),
                    name: normalize_ident(&idents[1]),
                })
            } else if idents.len() >= 3 {
                let schema = normalize_ident(&idents[idents.len() - 3]);
                let table = normalize_ident(&idents[idents.len() - 2]);
                Some(ColumnRef {
                    relation: Some(format!("{}.{}", schema, table)),
                    name: normalize_ident(&idents[idents.len() - 1]),
                })
            } else {
                None
            }
        }
        _ => None,
    }
}

fn resolve_table_name(
    relation: Option<&str>,
    alias_map: &HashMap<String, String>,
    default_table: &Option<String>,
) -> Option<String> {
    match relation {
        Some(rel) => alias_map.get(rel).cloned(),
        None => default_table.clone(),
    }
}

fn parse_object_name(name: &sqlparser::ast::ObjectName) -> (Option<String>, String) {
    let parts: Vec<String> = name.0.iter().map(normalize_ident).collect();
    match parts.len() {
        1 => (None, parts[0].clone()),
        2 => (Some(parts[0].clone()), parts[1].clone()),
        _ => {
            let schema = parts[parts.len() - 2].clone();
            let table = parts[parts.len() - 1].clone();
            (Some(schema), table)
        }
    }
}

fn normalize_ident(ident: &sqlparser::ast::Ident) -> String {
    normalize_identifier(&ident.value, ident.quote_style)
}

fn normalize_identifier(value: &str, quote_style: Option<char>) -> String {
    if quote_style.is_some() {
        value.to_string()
    } else {
        value.to_ascii_lowercase()
    }
}

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(value))
    {
        values.push(value.to_string());
    }
}

fn merge_table_usage(target: &mut TableColumnUsage, source: &TableColumnUsage) {
    for value in &source.equality_filters {
        push_unique(&mut target.equality_filters, value);
    }
    for value in &source.non_equality_filters {
        push_unique(&mut target.non_equality_filters, value);
    }
    for value in &source.equality_joins {
        push_unique(&mut target.equality_joins, value);
    }
    for value in &source.orders {
        push_unique(&mut target.orders, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_single_table_filters_and_orders() {
        let query = "SELECT * FROM orders WHERE customer_id = $1 ORDER BY created_at";
        let usage = parse_query_columns(query).expect("parse");
        assert_eq!(usage.tables.len(), 1);
        let table = usage.tables[0].full_name();
        let table_usage = usage.usage_by_table.get(&table).expect("table usage");
        assert!(table_usage
            .equality_filters
            .iter()
            .any(|c| c == "customer_id"));
        assert!(table_usage.orders.iter().any(|c| c == "created_at"));
    }

    #[test]
    fn extracts_join_columns_with_aliases() {
        let query = "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id WHERE o.status = 'open'";
        let usage = parse_query_columns(query).expect("parse");
        assert_eq!(usage.tables.len(), 2);
        let orders = usage
            .usage_by_table
            .iter()
            .find(|(k, _)| k.ends_with("orders"))
            .map(|(_, v)| v)
            .expect("orders");
        assert!(orders.equality_joins.iter().any(|c| c == "customer_id"));
        assert!(orders.equality_filters.iter().any(|c| c == "status"));
    }

    #[test]
    fn skips_unqualified_columns_when_multiple_tables() {
        let query =
            "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id WHERE status = 'open'";
        let usage = parse_query_columns(query).expect("parse");
        let orders = usage
            .usage_by_table
            .iter()
            .find(|(k, _)| k.ends_with("orders"))
            .map(|(_, v)| v)
            .expect("orders");
        assert!(!orders.equality_filters.iter().any(|c| c == "status"));
    }

    #[test]
    fn extracts_using_join_columns() {
        let query = "SELECT * FROM orders o JOIN customers c USING (customer_id)";
        let usage = parse_query_columns(query).expect("parse");
        let orders = usage
            .usage_by_table
            .iter()
            .find(|(k, _)| k.ends_with("orders"))
            .map(|(_, v)| v)
            .expect("orders");
        let customers = usage
            .usage_by_table
            .iter()
            .find(|(k, _)| k.ends_with("customers"))
            .map(|(_, v)| v)
            .expect("customers");
        assert!(orders.equality_joins.iter().any(|c| c == "customer_id"));
        assert!(customers.equality_joins.iter().any(|c| c == "customer_id"));
    }

    #[test]
    fn parses_update_where_columns() {
        let query = "UPDATE orders SET status = 'closed' WHERE customer_id = $1";
        let usage = parse_query_columns(query).expect("parse");
        let orders = usage
            .usage_by_table
            .iter()
            .find(|(k, _)| k.ends_with("orders"))
            .map(|(_, v)| v)
            .expect("orders");
        assert!(orders.equality_filters.iter().any(|c| c == "customer_id"));
    }

    #[test]
    fn parses_update_from_join_columns() {
        let query = "UPDATE orders o SET status = 'closed' FROM customers c WHERE o.customer_id = c.id AND c.region = 'us'";
        let usage = parse_query_columns(query).expect("parse");
        let orders = usage
            .usage_by_table
            .iter()
            .find(|(k, _)| k.ends_with("orders"))
            .map(|(_, v)| v)
            .expect("orders");
        let customers = usage
            .usage_by_table
            .iter()
            .find(|(k, _)| k.ends_with("customers"))
            .map(|(_, v)| v)
            .expect("customers");
        assert!(orders.equality_joins.iter().any(|c| c == "customer_id"));
        assert!(customers.equality_joins.iter().any(|c| c == "id"));
        assert!(customers.equality_filters.iter().any(|c| c == "region"));
    }

    #[test]
    fn parses_delete_where_columns() {
        let query = "DELETE FROM orders WHERE customer_id = $1";
        let usage = parse_query_columns(query).expect("parse");
        let orders = usage
            .usage_by_table
            .iter()
            .find(|(k, _)| k.ends_with("orders"))
            .map(|(_, v)| v)
            .expect("orders");
        assert!(orders.equality_filters.iter().any(|c| c == "customer_id"));
    }

    #[test]
    fn parses_delete_using_join_columns() {
        let query =
            "DELETE FROM orders o USING customers c WHERE o.customer_id = c.id AND c.region = 'us'";
        let usage = parse_query_columns(query).expect("parse");
        let orders = usage
            .usage_by_table
            .iter()
            .find(|(k, _)| k.ends_with("orders"))
            .map(|(_, v)| v)
            .expect("orders");
        let customers = usage
            .usage_by_table
            .iter()
            .find(|(k, _)| k.ends_with("customers"))
            .map(|(_, v)| v)
            .expect("customers");
        assert!(orders.equality_joins.iter().any(|c| c == "customer_id"));
        assert!(customers.equality_joins.iter().any(|c| c == "id"));
        assert!(customers.equality_filters.iter().any(|c| c == "region"));
    }

    #[test]
    fn derived_subquery_alias_does_not_leak() {
        let query = "SELECT * FROM orders o JOIN (SELECT customer_id FROM customers o WHERE o.region = 'us') d ON d.customer_id = o.customer_id WHERE o.status = 'open'";
        let usage = parse_query_columns(query).expect("parse");
        let orders = usage
            .usage_by_table
            .iter()
            .find(|(k, _)| k.ends_with("orders"))
            .map(|(_, v)| v)
            .expect("orders");
        let customers = usage
            .usage_by_table
            .iter()
            .find(|(k, _)| k.ends_with("customers"))
            .map(|(_, v)| v)
            .expect("customers");
        assert!(orders.equality_filters.iter().any(|c| c == "status"));
        assert!(!customers.equality_filters.iter().any(|c| c == "status"));
    }

    #[test]
    fn alias_lookup_is_case_insensitive_for_unquoted_identifiers() {
        let query = "SELECT * FROM orders O WHERE o.customer_id = $1";
        let usage = parse_query_columns(query).expect("parse");
        let orders = usage
            .usage_by_table
            .iter()
            .find(|(k, _)| k.ends_with("orders"))
            .map(|(_, v)| v)
            .expect("orders");
        assert!(orders.equality_filters.iter().any(|c| c == "customer_id"));
    }

    #[test]
    fn quoted_alias_remains_case_sensitive() {
        let query = "SELECT * FROM orders AS \"O\" WHERE o.customer_id = $1";
        let usage = parse_query_columns(query).expect("parse");
        let has_customer_filter = usage
            .usage_by_table
            .iter()
            .find(|(k, _)| k.ends_with("orders"))
            .map(|(_, table_usage)| {
                table_usage
                    .equality_filters
                    .iter()
                    .any(|c| c == "customer_id")
            })
            .unwrap_or(false);
        assert!(!has_customer_filter);
    }

    #[test]
    fn classifies_non_equality_filters_separately() {
        let query =
            "SELECT * FROM orders WHERE created_at BETWEEN $1 AND $2 AND archived_at IS NULL";
        let usage = parse_query_columns(query).expect("parse");
        let orders = usage
            .usage_by_table
            .iter()
            .find(|(k, _)| k.ends_with("orders"))
            .map(|(_, v)| v)
            .expect("orders");
        assert!(orders
            .non_equality_filters
            .iter()
            .any(|c| c == "created_at"));
        assert!(orders
            .non_equality_filters
            .iter()
            .any(|c| c == "archived_at"));
    }
}
