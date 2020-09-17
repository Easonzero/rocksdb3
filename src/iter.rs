use pyo3::class::iter::PyIterProtocol;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use rocksdb::{DBIterator, DB};

#[pyclass]
pub struct RocksDBIterator {
    // FIXME: missing lifetime specifier
    // pyo3 not support non-copy iterator right now:
    // - https://github.com/PyO3/pyo3/issues/1085
    // - https://github.com/PyO3/pyo3/issues/1089
    db: DB,
    inner: DBIterator,
}

#[pyproto]
impl PyIterProtocol for RocksDBIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> PyResult<Option<PyObject>> {
        let mut iter = slf.inner.next();
        for (key, value) in iter {
            let py = slf.py();
            return Ok(Some(
                PyTuple::new(
                    py,
                    &[
                        PyBytes::new(py, key.as_ref()),
                        PyBytes::new(py, value.as_ref()),
                    ],
                )
                .into_py(py),
            ));
        }
        return Ok(None);
    }
}
