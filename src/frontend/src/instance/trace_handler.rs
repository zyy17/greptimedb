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
use common_catalog::consts::{default_engine, TRACE_TABLE_NAME};
use common_error::ext::BoxedError;
use common_telemetry::info;
use servers::error::{
    CatalogSnafu, ExecuteGrpcRequestSnafu, InFlightWriteBytesExceededSnafu, Result as ServerResult,
    TableOperationSnafu,
};
use session::context::QueryContextRef;
use snafu::ResultExt;
use store_api::mito_engine_options::APPEND_MODE_KEY;
use table::TableRef;
use tokio::sync::OnceCell;

use crate::instance::Instance;

const OTEL_TRACES_SERVICES_RED_METRICS_1M_TABLE_NAME: &str = "otel_traces_services_red_metrics_1m";

static START: OnceCell<()> = OnceCell::const_new();

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

        START
            .get_or_init(|| async {
                self.create_builtin_traces_flows(ctx.clone()).await.unwrap();
            })
            .await;

        self.create_flows(ctx).await.unwrap();

        result
    }

    async fn create_builtin_traces_flows(&self, ctx: QueryContextRef) -> ServerResult<TableRef> {
        if let Some(table) = self
            .catalog_manager
            .table(
                ctx.current_catalog(),
                "public",
                OTEL_TRACES_SERVICES_RED_METRICS_1M_TABLE_NAME,
                Some(&ctx),
            )
            .await
            .context(CatalogSnafu)?
        {
            return Ok(table);
        }

        // Create the system table if it doesn't exist.
        self.create_sink_tables(ctx.clone()).await
    }

    async fn create_sink_tables(&self, query_ctx: QueryContextRef) -> ServerResult<TableRef> {
        let mut create_table_expr = self.build_create_table_expr(query_ctx.current_catalog());
        if let Some(table) = self
            .catalog_manager
            .table(
                &create_table_expr.catalog_name,
                &create_table_expr.schema_name,
                &create_table_expr.table_name,
                Some(&query_ctx),
            )
            .await
            .context(CatalogSnafu)?
        {
            // The table is already created, so we don't need to create it again.
            return Ok(table);
        }

        // Create the `slow_queries` system table.
        let table = self
            .statement_executor
            .create_table_inner(&mut create_table_expr, None, query_ctx.clone())
            .await
            .context(TableOperationSnafu)?;

        info!(
            "Create the {} table in {:?} successfully.",
            OTEL_TRACES_SERVICES_RED_METRICS_1M_TABLE_NAME, "public"
        );

        Ok(table)
    }

    fn build_create_table_expr(&self, catalog: &str) -> CreateTableExpr {
        let column_defs = vec![
            ColumnDef {
                name: "service_name".to_string(),
                data_type: ColumnDataType::String as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "The name of the service".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: "total_count".to_string(),
                data_type: ColumnDataType::Int64 as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "The total count of traces".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: "error_count".to_string(),
                data_type: ColumnDataType::Int64 as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "The error count of traces".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: "avg_latency_nano".to_string(),
                data_type: ColumnDataType::Float64 as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "The average latency of traces in nanoseconds".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: "latency_sketch".to_string(),
                data_type: ColumnDataType::Binary as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Field as i32,
                comment: "The latency sketch of traces".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: "time_window".to_string(),
                data_type: ColumnDataType::TimestampNanosecond as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Timestamp as i32,
                comment: "The time window of the traces".to_string(),
                datatype_extension: None,
                options: None,
            },
            ColumnDef {
                name: "updated_at".to_string(),
                data_type: ColumnDataType::TimestampNanosecond as i32,
                is_nullable: false,
                default_constraint: vec![],
                semantic_type: SemanticType::Timestamp as i32,
                comment: "The updated time of the aggregation".to_string(),
                datatype_extension: None,
                options: None,
            },
        ];

        let table_options = HashMap::from([(APPEND_MODE_KEY.to_string(), "true".to_string())]);

        CreateTableExpr {
            catalog_name: catalog.to_string(),
            schema_name: "public".to_string(),
            table_name: OTEL_TRACES_SERVICES_RED_METRICS_1M_TABLE_NAME.to_string(),
            column_defs,
            time_index: "time_window".to_string(),
            primary_keys: vec![],
            create_if_not_exists: true,
            table_options,
            table_id: None,
            desc: "GreptimeDB system table for storing traces".to_string(),
            engine: default_engine().to_string(),
        }
    }

    async fn create_flows(&self, query_ctx: QueryContextRef) -> ServerResult<Output> {
        let create_flow_expr = self.build_create_flow_expr(query_ctx.current_catalog());

        // Create the `slow_queries` system table.
        let table = self
            .statement_executor
            .create_flow_inner(create_flow_expr, query_ctx.clone())
            .await
            .context(TableOperationSnafu)?;

        Ok(table)
    }

    fn build_create_flow_expr(&self, catalog: &str) -> CreateFlowExpr {
        let sql = r#"SELECT
        service_name,
        count(*) as total_count,
        sum(case when span_status_code = 'STATUS_CODE_ERROR' then 1 else 0 end) as error_count,
        avg(duration_nano) as avg_latency_nano,
        uddsketch_state(128, 0.01, duration_nano) AS latency_sketch,
        date_bin('1 minutes'::INTERVAL, timestamp, '2025-05-17 00:00:00') as time_window
    FROM opentelemetry_traces GROUP BY service_name, time_window"#;

        CreateFlowExpr {
            catalog_name: catalog.to_string(),
            flow_name: "test_flow".to_string(),
            source_table_names: vec![TableName {
                catalog_name: catalog.to_string(),
                schema_name: "public".to_string(),
                table_name: TRACE_TABLE_NAME.to_string(),
            }],
            flow_options: HashMap::new(),
            sink_table_name: Some(TableName {
                catalog_name: catalog.to_string(),
                schema_name: "public".to_string(),
                table_name: OTEL_TRACES_SERVICES_RED_METRICS_1M_TABLE_NAME.to_string(),
            }),
            or_replace: false,
            create_if_not_exists: true,
            expire_after: None,
            comment: "".to_string(),
            sql: sql.to_string(),
        }
    }
}
