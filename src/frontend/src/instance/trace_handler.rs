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

use api::v1::RowInsertRequests;
use client::Output;
use common_error::ext::BoxedError;
use servers::error::{
    ExecuteGrpcRequestSnafu, InFlightWriteBytesExceededSnafu, Result as ServerResult,
};
use session::context::QueryContextRef;
use snafu::ResultExt;

use crate::instance::Instance;

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

        self.inserter
            .handle_trace_inserts(rows, ctx, self.statement_executor.as_ref())
            .await
            .map_err(BoxedError::new)
            .context(ExecuteGrpcRequestSnafu)
    }
}
