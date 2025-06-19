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

use api::v1::{
    ColumnDataType, ColumnDef, CreateFlowExpr, CreateTableExpr, RowInsertRequests, SemanticType,
    TableName,
};
use client::Output;
use common_catalog::consts::{
    default_engine, PARENT_SPAN_ID_COLUMN, SERVICE_NAME_COLUMN, SPAN_ID_COLUMN, TRACE_TABLE_NAME,
};
use common_error::ext::BoxedError;
use common_telemetry::{debug, info, warn};
use servers::error::{
    CatalogSnafu, ExecuteGrpcRequestSnafu, InFlightWriteBytesExceededSnafu, Result as ServerResult,
    TableOperationSnafu,
};
use servers::otlp::trace::TIMESTAMP_COLUMN;
use session::context::QueryContextRef;
use snafu::ResultExt;
use table::TableRef;
use tokio::sync::OnceCell;

use crate::instance::Instance;

const OPENTELEMETRY_TRACES_DEPENDENCIES_1H_TABLE_NAME: &str =
    "opentelemetry_traces_dependencies_1h";

const PARENT_SERVICE_COLUMN: &str = "parent_service";
const CHILD_SERVICE_COLUMN: &str = "child_service";
const CALL_COUNT_COLUMN: &str = "call_count";
const TIME_WINDOW_COLUMN: &str = "time_window";
const UPDATED_AT_COLUMN: &str = "updated_at";

const OPENTELEMETRY_TRACES_DEPENDENCIES_1H_FLOW_NAME: &str =
    "opentelemetry_traces_dependencies_1h_flow";

static CREATE_BUILT_IN_FLOWS: OnceCell<()> = OnceCell::const_new();

impl Instance {
    pub async fn handle_trace_inserts(
        &self,
        rows: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> ServerResult<Output> {
        let _guard = if let Some(limiter) = &self.limiter {
            let result = limiter.limit_row_inserts(&rows);
            if result.is_none() {
                return InFlightWriteBytesExceededSnafu.fail();
            }
            result
        } else {
            None
        };

        let result = self
            .inserter
            .handle_trace_inserts(rows, ctx.clone(), self.statement_executor.as_ref())
            .await
            .map_err(BoxedError::new)
            .context(ExecuteGrpcRequestSnafu);

        CREATE_BUILT_IN_FLOWS
            .get_or_init(|| async {
                self.create_sink_tables(ctx.clone()).await;
                self.create_flows(ctx.clone()).await;
            })
            .await;

        result
    }

    async fn create_sink_tables(&self, query_ctx: QueryContextRef) {
        for table_expr in
            self.create_table_exprs(query_ctx.current_catalog(), &query_ctx.current_schema())
        {
            let catalog_name = &query_ctx.current_catalog();
            let schema_name = &query_ctx.current_schema();
            let table_name = table_expr.table_name.clone();

            debug!(
                "Creating the table '{}' in schema '{}' catalog '{}'",
                table_name, schema_name, catalog_name
            );

            if let Err(e) = self.do_create_table(query_ctx.clone(), table_expr).await {
                warn!(e; "Failed to create table '{}' in schema '{}' catalog '{}'",
                    table_name, schema_name, catalog_name
                );
            }
        }
    }

    async fn do_create_table(
        &self,
        query_ctx: QueryContextRef,
        mut table_expr: CreateTableExpr,
    ) -> ServerResult<TableRef> {
        debug!("Creating the table for built-in flows '{:?}'", table_expr);

        if let Some(table) = self
            .catalog_manager
            .table(
                &table_expr.catalog_name,
                &table_expr.schema_name,
                &table_expr.table_name,
                Some(&query_ctx),
            )
            .await
            .context(CatalogSnafu)?
        {
            // The table is already created, so we don't need to create it again.
            return Ok(table);
        }

        // Create the table.
        let table = self
            .statement_executor
            .create_table_inner(&mut table_expr, None, query_ctx.clone())
            .await
            .context(TableOperationSnafu)?;

        info!(
            "Create the '{}' table in schema '{}' catalog '{}' successfully.",
            table_expr.table_name, table_expr.schema_name, table_expr.catalog_name
        );

        Ok(table)
    }

    fn create_table_exprs(&self, catalog: &str, schema: &str) -> Vec<CreateTableExpr> {
        vec![self.dependencies_1h_table_create_table_expr(catalog, schema)]
    }

    fn dependencies_1h_table_create_table_expr(
        &self,
        catalog: &str,
        schema: &str,
    ) -> CreateTableExpr {
        let column_defs = vec![
            ColumnDef {
                name: PARENT_SERVICE_COLUMN.to_string(),
                data_type: ColumnDataType::String as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "The name of the parent service".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: CHILD_SERVICE_COLUMN.to_string(),
                data_type: ColumnDataType::String as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "The name of the child service".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: CALL_COUNT_COLUMN.to_string(),
                data_type: ColumnDataType::Int64 as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "The number of calls between parent and child services".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: TIME_WINDOW_COLUMN.to_string(),
                data_type: ColumnDataType::TimestampNanosecond as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Timestamp as i32,
                comment: "The time window of the traces".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: UPDATED_AT_COLUMN.to_string(),
                data_type: ColumnDataType::TimestampNanosecond as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Timestamp as i32,
                comment: "The updated time of the aggregation".to_string(),
                datatype_extension: None,
                options: None,
            },
        ];

        CreateTableExpr {
            catalog_name: catalog.to_string(),
            schema_name: schema.to_string(),
            table_name: OPENTELEMETRY_TRACES_DEPENDENCIES_1H_TABLE_NAME.to_string(),
            column_defs,
            time_index: TIME_WINDOW_COLUMN.to_string(),
            primary_keys: vec![],
            create_if_not_exists: true,
            table_options: HashMap::new(),
            table_id: None,
            desc: "The table for storing traces dependencies for 1 hour aggregation".to_string(),
            engine: default_engine().to_string(),
        }
    }

    async fn create_flows(&self, query_ctx: QueryContextRef) {
        for flow_expr in
            self.create_flow_exprs(query_ctx.current_catalog(), &query_ctx.current_schema())
        {
            let catalog_name = &query_ctx.current_catalog();
            let schema_name = &query_ctx.current_schema();
            let flow_name = flow_expr.flow_name.clone();

            debug!(
                "Creating the flow '{}' in schema '{}' catalog '{}'",
                flow_name, schema_name, catalog_name
            );

            if let Err(e) = self.do_create_flow(query_ctx.clone(), flow_expr).await {
                warn!(e; "Failed to create the flow '{}' in schema '{}' catalog '{}'",
                    flow_name, schema_name, catalog_name
                );
            }
        }
    }

    async fn do_create_flow(
        &self,
        query_ctx: QueryContextRef,
        create_flow_expr: CreateFlowExpr,
    ) -> ServerResult<Output> {
        debug!("Creating the flow '{:?}'", create_flow_expr);

        // FIXME(zyy17): How to make sure the flow is initialized?
        self.statement_executor
            .create_flow_inner(create_flow_expr, query_ctx.clone())
            .await
            .context(TableOperationSnafu)
    }

    fn create_flow_exprs(&self, catalog: &str, schema: &str) -> Vec<CreateFlowExpr> {
        vec![self.dependencies_1h_create_flow_expr(catalog, schema)]
    }

    fn dependencies_1h_create_flow_expr(&self, catalog: &str, schema: &str) -> CreateFlowExpr {
        let sql = format!(
            r#"SELECT
                parent.{} as parent_service,
                child.{} as child_service,
                count(*) as {},
                date_bin('1 hours'::INTERVAL, parent.{}, '2025-05-17 00:00:00') as {}
            FROM {} as parent
            JOIN {} as child ON child.{} = parent.{}
            GROUP BY parent_service, child_service, {}"#,
            SERVICE_NAME_COLUMN,
            SERVICE_NAME_COLUMN,
            CALL_COUNT_COLUMN,
            TIMESTAMP_COLUMN,
            TIME_WINDOW_COLUMN,
            TRACE_TABLE_NAME,
            TRACE_TABLE_NAME,
            PARENT_SPAN_ID_COLUMN,
            SPAN_ID_COLUMN,
            TIME_WINDOW_COLUMN
        );

        CreateFlowExpr {
            catalog_name: catalog.to_string(),
            flow_name: OPENTELEMETRY_TRACES_DEPENDENCIES_1H_FLOW_NAME.to_string(),
            source_table_names: vec![TableName {
                catalog_name: catalog.to_string(),
                schema_name: schema.to_string(),
                table_name: TRACE_TABLE_NAME.to_string(),
            }],
            flow_options: HashMap::new(),
            sink_table_name: Some(TableName {
                catalog_name: catalog.to_string(),
                schema_name: schema.to_string(),
                table_name: OPENTELEMETRY_TRACES_DEPENDENCIES_1H_TABLE_NAME.to_string(),
            }),
            or_replace: false,
            create_if_not_exists: true,
            expire_after: None,
            comment: "The flow for aggregating traces dependencies in 1 hour window".to_string(),
            sql: sql.to_string(),
        }
    }
}
