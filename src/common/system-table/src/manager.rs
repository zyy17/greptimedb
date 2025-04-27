// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use api::v1::{CreateTableExpr, RowInsertRequests};
use catalog::CatalogManagerRef;
use common_catalog::format_full_table_name;
use common_query::Output;
use operator::insert::InserterRef;
use operator::statement::StatementExecutorRef;
use session::context::{QueryContextBuilder, QueryContextRef};
use snafu::{OptionExt, ResultExt};
use table::metadata::TableInfo;
use table::TableRef;

use crate::error::{CatalogSnafu, CreateTableSnafu, InsertRowsSnafu, Result, TableNotFoundSnafu};

pub type SystemTableManagerRef = Arc<SystemTableManager>;

/// SystemTableManager is responsible for managing system tables that are stored in object storage.
pub struct SystemTableManager {
    inserter: InserterRef,
    statement_executor: StatementExecutorRef,
    catalog_manager: CatalogManagerRef,
    tables: RwLock<HashMap<String, TableRef>>,
}

impl SystemTableManager {
    /// Create a new SystemTableManager.
    pub fn new(
        inserter: InserterRef,
        statement_executor: StatementExecutorRef,
        catalog_manager: CatalogManagerRef,
    ) -> Self {
        Self {
            inserter,
            statement_executor,
            catalog_manager,
            tables: RwLock::new(HashMap::new()),
        }
    }

    /// Create a system table.
    pub async fn create_system_table(
        &self,
        ctx: QueryContextRef,
        mut create_table_expr: CreateTableExpr,
    ) -> Result<TableRef> {
        // Check if the table already exists in the cache.
        if let Some(table) = self.tables.read().unwrap().get(&format_full_table_name(
            &create_table_expr.catalog_name,
            &create_table_expr.schema_name,
            &create_table_expr.table_name,
        )) {
            return Ok(table.clone());
        }

        if let Some(table) = self
            .catalog_manager
            .table(
                &create_table_expr.catalog_name,
                &create_table_expr.schema_name,
                &create_table_expr.table_name,
                Some(&ctx),
            )
            .await
            .context(CatalogSnafu)?
        {
            // Put table to cache.
            self.tables.write().unwrap().insert(
                format_full_table_name(
                    &create_table_expr.catalog_name,
                    &create_table_expr.schema_name,
                    &create_table_expr.table_name,
                ),
                table.clone(),
            );

            return Ok(table);
        }

        // Create table.
        self.statement_executor
            .create_table_inner(&mut create_table_expr, None, ctx.clone())
            .await
            .context(CreateTableSnafu)?;

        // Get table.
        let table = self
            .catalog_manager
            .table(
                &create_table_expr.catalog_name,
                &create_table_expr.schema_name,
                &create_table_expr.table_name,
                Some(&ctx),
            )
            .await
            .context(CatalogSnafu)?
            .context(TableNotFoundSnafu {
                catalog: create_table_expr.catalog_name.clone(),
                schema: create_table_expr.schema_name.clone(),
                table: create_table_expr.table_name.clone(),
            })?;

        Ok(table)
    }

    pub async fn get_system_table(
        &self,
        ctx: QueryContextRef,
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
    ) -> Result<TableRef> {
        let key = format_full_table_name(catalog_name, schema_name, table_name);
        if let Some(table) = self.tables.read().unwrap().get(&key) {
            return Ok(table.clone());
        }

        if let Some(table) = self
            .catalog_manager
            .table(catalog_name, schema_name, table_name, Some(&ctx))
            .await
            .context(CatalogSnafu)?
        {
            // Put table to cache.
            self.tables.write().unwrap().insert(key, table.clone());
            return Ok(table);
        }

        TableNotFoundSnafu {
            catalog: catalog_name.to_string(),
            schema: schema_name.to_string(),
            table: table_name.to_string(),
        }
        .fail()
    }

    /// Insert rows into a system table.
    pub async fn insert_rows(
        &self,
        table: TableRef,
        requests: RowInsertRequests,
    ) -> Result<Output> {
        let output = self
            .inserter
            .handle_row_inserts(
                requests,
                query_ctx(&table.table_info()),
                &self.statement_executor,
            )
            .await
            .context(InsertRowsSnafu)?;

        Ok(output)
    }
}

/// Create a query context from table info.
pub fn query_ctx(table_info: &TableInfo) -> QueryContextRef {
    QueryContextBuilder::default()
        .current_catalog(table_info.catalog_name.to_string())
        .current_schema(table_info.schema_name.to_string())
        .build()
        .into()
}
