use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use dkdc_db_client::{ColumnInfo, DbClient, QueryResponse};

fn to_py_err(e: dkdc_db_client::Error) -> PyErr {
    PyErr::new::<PyRuntimeError, _>(e.to_string())
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("failed to create tokio runtime")
}

fn json_value_to_py(py: Python<'_>, val: &serde_json::Value) -> Py<PyAny> {
    match val {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(b) => (*b)
            .into_pyobject(py)
            .unwrap()
            .to_owned()
            .unbind()
            .into_any(),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i.into_pyobject(py).unwrap().unbind().into_any()
            } else {
                n.as_f64()
                    .unwrap_or(0.0)
                    .into_pyobject(py)
                    .unwrap()
                    .unbind()
                    .into_any()
            }
        }
        serde_json::Value::String(s) => s.into_pyobject(py).unwrap().unbind().into_any(),
        serde_json::Value::Array(arr) => {
            let items: Vec<Py<PyAny>> = arr.iter().map(|v| json_value_to_py(py, v)).collect();
            PyList::new(py, items).unwrap().unbind().into_any()
        }
        serde_json::Value::Object(map) => {
            let dict = PyDict::new(py);
            for (k, v) in map {
                dict.set_item(k, json_value_to_py(py, v)).unwrap();
            }
            dict.unbind().into_any()
        }
    }
}

fn column_info_to_py(py: Python<'_>, col: &ColumnInfo) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("name", &col.name)?;
    dict.set_item("type", &col.r#type)?;
    Ok(dict.unbind().into_any())
}

fn query_response_to_py(py: Python<'_>, resp: QueryResponse) -> PyResult<Py<PyAny>> {
    let columns: Vec<Py<PyAny>> = resp
        .columns
        .iter()
        .map(|c| column_info_to_py(py, c))
        .collect::<PyResult<_>>()?;

    let rows: Vec<Py<PyAny>> = resp
        .rows
        .iter()
        .map(|row| {
            let items: Vec<Py<PyAny>> = row.iter().map(|v| json_value_to_py(py, v)).collect();
            PyList::new(py, items).unwrap().unbind().into_any()
        })
        .collect();

    let dict = PyDict::new(py);
    dict.set_item("columns", PyList::new(py, columns)?)?;
    dict.set_item("rows", PyList::new(py, rows)?)?;
    Ok(dict.unbind().into_any())
}

#[pyclass]
struct Db {
    client: DbClient,
}

#[pymethods]
impl Db {
    #[new]
    #[pyo3(signature = (url="http://127.0.0.1:4200"))]
    fn new(url: &str) -> Self {
        Self {
            client: DbClient::new(url),
        }
    }

    fn create_db(&self, name: &str) -> PyResult<()> {
        runtime()
            .block_on(self.client.create_db(name))
            .map_err(to_py_err)
    }

    fn drop_db(&self, name: &str) -> PyResult<()> {
        runtime()
            .block_on(self.client.drop_db(name))
            .map_err(to_py_err)
    }

    fn list_dbs(&self) -> PyResult<Vec<String>> {
        runtime()
            .block_on(self.client.list_dbs())
            .map_err(to_py_err)
    }

    fn execute(&self, db: &str, sql: &str) -> PyResult<u64> {
        runtime()
            .block_on(self.client.execute(db, sql))
            .map_err(to_py_err)
    }

    fn query(&self, py: Python<'_>, sql: &str) -> PyResult<Py<PyAny>> {
        let resp = runtime()
            .block_on(self.client.query(sql))
            .map_err(to_py_err)?;
        query_response_to_py(py, resp)
    }

    fn query_oltp(&self, py: Python<'_>, db: &str, sql: &str) -> PyResult<Py<PyAny>> {
        let resp = runtime()
            .block_on(self.client.query_oltp(db, sql))
            .map_err(to_py_err)?;
        query_response_to_py(py, resp)
    }

    fn list_tables(&self, db: &str) -> PyResult<Vec<String>> {
        runtime()
            .block_on(self.client.list_tables(db))
            .map_err(to_py_err)
    }

    fn table_schema(&self, py: Python<'_>, db: &str, table: &str) -> PyResult<Py<PyAny>> {
        let resp = runtime()
            .block_on(self.client.table_schema(db, table))
            .map_err(to_py_err)?;
        query_response_to_py(py, resp)
    }

    fn health(&self) -> PyResult<bool> {
        runtime()
            .block_on(self.client.health())
            .map_err(to_py_err)
    }
}

#[pyfunction]
fn run_cli() {
    dkdc_db_cli::run();
}

#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Db>()?;
    m.add_function(wrap_pyfunction!(run_cli, m)?)?;
    Ok(())
}
