use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::catalog::TableProvider;
use datafusion::datasource::TableType;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;

use crate::plan::SqliteScanExec;

pub struct SqliteTableProvider {
    table_name: String,
    schema: SchemaRef,
    conn: libsql::Connection,
}

impl SqliteTableProvider {
    pub fn new(table_name: String, schema: SchemaRef, conn: libsql::Connection) -> Self {
        Self {
            table_name,
            schema,
            conn,
        }
    }
}

impl fmt::Debug for SqliteTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteTableProvider")
            .field("table_name", &self.table_name)
            .finish()
    }
}

#[async_trait]
impl TableProvider for SqliteTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SqliteScanExec::new(
            self.table_name.clone(),
            self.schema.clone(),
            projection.cloned(),
            self.conn.clone(),
        )))
    }
}
