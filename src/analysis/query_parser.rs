use sqlparser::ast::{
    BinaryOperator, Expr, Join, JoinConstraint, OrderByExpr, Query, Select, SelectItem, SetExpr,
    Statement, TableFactor, TableWithJoins,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
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
    pub filters: Vec<String>,
    pub joins: Vec<String>,
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
    Filter,
    Join,
    Order,
}

pub fn parse_query_columns(
    query: &str,
) -> Result<QueryColumnUsage, sqlparser::parser::ParserError> {
    let dialect = PostgreSqlDialect {};
    let mut statements = Parser::parse_sql(&dialect, query)?;

    let statement = statements
        .pop()
        .ok_or_else(|| sqlparser::parser::ParserError::ParserError("empty query".into()))?;

    let mut collector = QueryColumnCollector::default();
    match statement {
        Statement::Query(query) => collector.collect_query(&query),
        _ => {
            return Err(sqlparser::parser::ParserError::ParserError(
                "unsupported statement".into(),
            ))
        }
    }

    Ok(collector.into_usage())
}

#[derive(Default)]
struct QueryColumnCollector {
    tables: Vec<TableRef>,
    alias_map: HashMap<String, String>,
    pending: Vec<PendingColumn>,
}

impl QueryColumnCollector {
    fn collect_query(&mut self, query: &Query) {
        self.collect_set_expr(&query.body);

        for order in &query.order_by {
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
                        .insert(alias.name.value.clone(), full_name.clone());
                }
                Some(full_name)
            }
            TableFactor::Derived { subquery, .. } => {
                self.collect_query(subquery.as_ref());
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
                                name: column.value.clone(),
                                kind: ColumnKind::Join,
                            });
                        }
                        if let Some(table) = &right_table {
                            self.pending.push(PendingColumn {
                                relation: Some(table.clone()),
                                name: column.value.clone(),
                                kind: ColumnKind::Join,
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
                    self.push_column_if_applicable(left, ColumnKind::Filter);
                    self.push_column_if_applicable(right, ColumnKind::Filter);
                }
                _ => {}
            },
            Expr::InList { expr, .. } => self.push_column_if_applicable(expr, ColumnKind::Filter),
            Expr::Between { expr, .. } => self.push_column_if_applicable(expr, ColumnKind::Filter),
            Expr::IsNull(expr) | Expr::IsNotNull(expr) => {
                self.push_column_if_applicable(expr, ColumnKind::Filter)
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
                    self.push_column_if_applicable(left, ColumnKind::Join);
                    self.push_column_if_applicable(right, ColumnKind::Join);
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

    fn into_usage(self) -> QueryColumnUsage {
        let mut usage = QueryColumnUsage {
            tables: self.tables.clone(),
            ..Default::default()
        };

        let default_table = if self.tables.len() == 1 {
            Some(self.tables[0].full_name())
        } else {
            None
        };

        for pending in self.pending {
            let table =
                resolve_table_name(pending.relation.as_deref(), &self.alias_map, &default_table);
            let Some(table_name) = table else { continue };
            let entry = usage.usage_by_table.entry(table_name).or_default();
            match pending.kind {
                ColumnKind::Filter => push_unique(&mut entry.filters, &pending.name),
                ColumnKind::Join => push_unique(&mut entry.joins, &pending.name),
                ColumnKind::Order => push_unique(&mut entry.orders, &pending.name),
            }
        }

        usage
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
            name: ident.value.clone(),
        }),
        Expr::CompoundIdentifier(idents) => {
            if idents.len() == 2 {
                Some(ColumnRef {
                    relation: Some(idents[0].value.clone()),
                    name: idents[1].value.clone(),
                })
            } else if idents.len() >= 3 {
                let schema = idents[idents.len() - 3].value.clone();
                let table = idents[idents.len() - 2].value.clone();
                Some(ColumnRef {
                    relation: Some(format!("{}.{}", schema, table)),
                    name: idents[idents.len() - 1].value.clone(),
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
        Some(rel) => alias_map
            .get(rel)
            .cloned()
            .or_else(|| alias_map.get(&rel.to_string()).cloned()),
        None => default_table.clone(),
    }
}

fn parse_object_name(name: &sqlparser::ast::ObjectName) -> (Option<String>, String) {
    let parts: Vec<String> = name.0.iter().map(|ident| ident.value.clone()).collect();
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

fn push_unique(values: &mut Vec<String>, value: &str) {
    if !values
        .iter()
        .any(|existing| existing.eq_ignore_ascii_case(value))
    {
        values.push(value.to_string());
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
        assert!(table_usage.filters.iter().any(|c| c == "customer_id"));
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
        assert!(orders.joins.iter().any(|c| c == "customer_id"));
        assert!(orders.filters.iter().any(|c| c == "status"));
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
        assert!(!orders.filters.iter().any(|c| c == "status"));
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
        assert!(orders.joins.iter().any(|c| c == "customer_id"));
        assert!(customers.joins.iter().any(|c| c == "customer_id"));
    }
}
