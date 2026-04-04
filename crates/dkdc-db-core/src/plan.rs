use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, PlanProperties, SendableRecordBatchStream,
};

use crate::convert::rows_to_record_batch;

pub struct SqliteScanExec {
    table_name: String,
    projected_schema: SchemaRef,
    full_schema: SchemaRef,
    projection: Option<Vec<usize>>,
    db: turso::Database,
    props: Arc<PlanProperties>,
}

impl SqliteScanExec {
    pub fn new(
        table_name: String,
        full_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        db: turso::Database,
    ) -> Self {
        let projected_schema = match &projection {
            Some(proj) if !proj.is_empty() => {
                let fields: Vec<_> = proj.iter().map(|&i| full_schema.field(i).clone()).collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            Some(_) => Arc::new(arrow::datatypes::Schema::empty()),
            None => full_schema.clone(),
        };

        let props = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(projected_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));

        Self {
            table_name,
            projected_schema,
            full_schema,
            projection,
            db,
            props,
        }
    }
}

impl fmt::Debug for SqliteScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqliteScanExec")
            .field("table_name", &self.table_name)
            .field("projection", &self.projection)
            .finish()
    }
}

impl datafusion::physical_plan::DisplayAs for SqliteScanExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut fmt::Formatter,
    ) -> fmt::Result {
        write!(f, "SqliteScanExec: table={}", self.table_name)
    }
}

impl ExecutionPlan for SqliteScanExec {
    fn name(&self) -> &str {
        "SqliteScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::error::Result<SendableRecordBatchStream> {
        let is_empty_projection = matches!(&self.projection, Some(proj) if proj.is_empty());

        let columns: Vec<String> = match &self.projection {
            Some(proj) if !proj.is_empty() => proj
                .iter()
                .map(|&i| format!("\"{}\"", self.full_schema.field(i).name()))
                .collect(),
            _ => self
                .full_schema
                .fields()
                .iter()
                .map(|f| format!("\"{}\"", f.name()))
                .collect(),
        };
        let col_list = if columns.is_empty() {
            "*".to_string()
        } else {
            columns.join(", ")
        };

        let db = self.db.clone();
        let schema = self.projected_schema.clone();
        let schema_for_stream = self.projected_schema.clone();
        let table_name = self.table_name.clone();

        let stream = futures::stream::once(async move {
            // Create a fresh connection for this scan to support concurrent reads
            let conn = db
                .connect()
                .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;

            if is_empty_projection {
                let count_sql = format!("SELECT count(*) FROM \"{table_name}\"");
                let mut rows = conn
                    .query(&count_sql, ())
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let row_count = if let Some(row) = rows
                    .next()
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                {
                    row.get::<i64>(0)
                        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
                        as usize
                } else {
                    0
                };
                let options =
                    arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(row_count));
                Ok::<RecordBatch, datafusion::error::DataFusionError>(
                    RecordBatch::try_new_with_options(schema, vec![], &options)?,
                )
            } else {
                let sql = format!("SELECT {col_list} FROM \"{table_name}\"");
                let mut rows = conn
                    .query(&sql, ())
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                let batch = rows_to_record_batch(&mut rows, schema)
                    .await
                    .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?;
                Ok(batch)
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema_for_stream,
            stream,
        )))
    }
}
