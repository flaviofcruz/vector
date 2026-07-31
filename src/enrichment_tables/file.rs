//! Handles enrichment tables for `type = file`.
use std::{collections::HashMap, fs, path::PathBuf, time::SystemTime};

use bytes::Bytes;
use tracing::trace;
use vector_lib::{
    TimeZone,
    configurable::configurable_component,
    conversion::Conversion,
    enrichment::{Case, Condition, IndexHandle, Table},
};
use vrl::value::{ObjectMap, Value};

use crate::config::EnrichmentTableConfig;
use crate::enrichment_tables::indexed_data::IndexedData;

/// File encoding configuration.
#[configurable_component]
#[derive(Clone, Debug, Eq, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[configurable(metadata(docs::enum_tag_description = "File encoding type."))]
pub enum Encoding {
    /// Decodes the file as a [CSV][csv] (comma-separated values) file.
    ///
    /// [csv]: https://wikipedia.org/wiki/Comma-separated_values
    Csv {
        /// Whether or not the file contains column headers.
        ///
        /// When set to `true`, the first row of the CSV file will be read as the header row, and
        /// the values will be used for the names of each column. This is the default behavior.
        ///
        /// When set to `false`, columns are referred to by their numerical index.
        #[serde(default = "crate::serde::default_true")]
        include_headers: bool,

        /// The delimiter used to separate fields in each row of the CSV file.
        #[serde(default = "default_delimiter")]
        delimiter: char,
    },

    /// Decodes the file as a JSON file.
    ///
    /// A top-level JSON object is treated as a single-row table where each key becomes a column.
    /// A top-level JSON array of objects is treated as a multi-row table.
    Json,
}

impl Default for Encoding {
    fn default() -> Self {
        Self::Csv {
            include_headers: true,
            delimiter: default_delimiter(),
        }
    }
}

/// File-specific settings.
#[configurable_component]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FileSettings {
    /// The path of the enrichment table file.
    ///
    /// Supported formats: [CSV][csv] and JSON.
    ///
    /// [csv]: https://en.wikipedia.org/wiki/Comma-separated_values
    pub path: PathBuf,

    /// File encoding configuration.
    #[configurable(derived)]
    pub encoding: Encoding,
}

/// Configuration for the `file` enrichment table.
#[configurable_component(enrichment_table("file"))]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FileConfig {
    /// File-specific settings.
    #[configurable(derived)]
    pub file: FileSettings,

    /// Key/value pairs representing mapped log field names and types.
    ///
    /// This is used to coerce log fields from strings into their proper types. The available types are listed in the `Types` list below.
    ///
    /// Timestamp coercions need to be prefaced with `timestamp|`, for example `"timestamp|%F"`. Timestamp specifiers can use either of the following:
    ///
    /// 1. One of the built-in-formats listed in the `Timestamp Formats` table below.
    /// 2. The [time format specifiers][chrono_fmt] from Rust’s `chrono` library.
    ///
    /// Types
    ///
    /// - **`bool`**
    /// - **`string`**
    /// - **`float`**
    /// - **`integer`**
    /// - **`date`**
    /// - **`timestamp`** (see the table below for formats)
    ///
    /// Timestamp Formats
    ///
    /// | Format               | Description                                                                      | Example                          |
    /// |----------------------|----------------------------------------------------------------------------------|----------------------------------|
    /// | `%F %T`              | `YYYY-MM-DD HH:MM:SS`                                                            | `2020-12-01 02:37:54`            |
    /// | `%v %T`              | `DD-Mmm-YYYY HH:MM:SS`                                                           | `01-Dec-2020 02:37:54`           |
    /// | `%FT%T`              | [ISO 8601][iso8601]/[RFC 3339][rfc3339], without time zone                       | `2020-12-01T02:37:54`            |
    /// | `%FT%TZ`             | [ISO 8601][iso8601]/[RFC 3339][rfc3339], UTC                                     | `2020-12-01T09:37:54Z`           |
    /// | `%+`                 | [ISO 8601][iso8601]/[RFC 3339][rfc3339], UTC, with time zone                     | `2020-12-01T02:37:54-07:00`      |
    /// | `%a, %d %b %Y %T`    | [RFC 822][rfc822]/[RFC 2822][rfc2822], without time zone                         | `Tue, 01 Dec 2020 02:37:54`      |
    /// | `%a %b %e %T %Y`     | [ctime][ctime] format                                                            | `Tue Dec 1 02:37:54 2020`        |
    /// | `%s`                 | [UNIX timestamp][unix_ts]                                                        | `1606790274`                     |
    /// | `%a %d %b %T %Y`     | [date][date] command, without time zone                                          | `Tue 01 Dec 02:37:54 2020`       |
    /// | `%a %d %b %T %Z %Y`  | [date][date] command, with time zone                                             | `Tue 01 Dec 02:37:54 PST 2020`   |
    /// | `%a %d %b %T %z %Y`  | [date][date] command, with numeric time zone                                     | `Tue 01 Dec 02:37:54 -0700 2020` |
    /// | `%a %d %b %T %#z %Y` | [date][date] command, with numeric time zone (minutes can be missing or present) | `Tue 01 Dec 02:37:54 -07 2020`   |
    ///
    /// [date]: https://man7.org/linux/man-pages/man1/date.1.html
    /// [ctime]: https://www.cplusplus.com/reference/ctime
    /// [unix_ts]: https://en.wikipedia.org/wiki/Unix_time
    /// [rfc822]: https://tools.ietf.org/html/rfc822#section-5
    /// [rfc2822]: https://tools.ietf.org/html/rfc2822#section-3.3
    /// [iso8601]: https://en.wikipedia.org/wiki/ISO_8601
    /// [rfc3339]: https://tools.ietf.org/html/rfc3339
    /// [chrono_fmt]: https://docs.rs/chrono/latest/chrono/format/strftime/index.html#specifiers
    #[serde(default)]
    #[configurable(metadata(
        docs::additional_props_description = "Represents mapped log field names and types."
    ))]
    pub schema: HashMap<String, String>,
}

const fn default_delimiter() -> char {
    ','
}

pub(crate) fn json_to_vrl_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(
                    ordered_float::NotNan::new(f)
                        .unwrap_or(ordered_float::NotNan::new(0.0_f64).unwrap()),
                )
            } else {
                n.to_string().as_str().into()
            }
        }
        serde_json::Value::String(s) => s.as_str().into(),
        // Nested arrays and objects are serialized back to their JSON string representation
        // so that the column value is human-readable and no data is silently lost.
        _ => serde_json::to_string(v).unwrap_or_default().as_str().into(),
    }
}

impl FileConfig {
    fn parse_column(
        &self,
        timezone: TimeZone,
        column: &str,
        row: usize,
        value: &str,
    ) -> Result<Value, String> {
        use chrono::TimeZone;

        Ok(match self.schema.get(column) {
            Some(format) => {
                let mut split = format.splitn(2, '|').map(|segment| segment.trim());

                match (split.next(), split.next()) {
                    (Some("date"), None) => Value::Timestamp(
                        chrono::FixedOffset::east_opt(0)
                            .expect("invalid timestamp")
                            .from_utc_datetime(
                                &chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d")
                                    .map_err(|_| {
                                        format!("unable to parse date {value} found in row {row}")
                                    })?
                                    .and_hms_opt(0, 0, 0)
                                    .expect("invalid timestamp"),
                            )
                            .into(),
                    ),
                    (Some("date"), Some(format)) => Value::Timestamp(
                        chrono::FixedOffset::east_opt(0)
                            .expect("invalid timestamp")
                            .from_utc_datetime(
                                &chrono::NaiveDate::parse_from_str(value, format)
                                    .map_err(|_| {
                                        format!("unable to parse date {value} found in row {row}")
                                    })?
                                    .and_hms_opt(0, 0, 0)
                                    .expect("invalid timestamp"),
                            )
                            .into(),
                    ),
                    _ => {
                        let conversion =
                            Conversion::parse(format, timezone).map_err(|err| err.to_string())?;
                        conversion
                            .convert(Bytes::copy_from_slice(value.as_bytes()))
                            .map_err(|_| format!("unable to parse {value} found in row {row}"))?
                    }
                }
            }
            None => value.into(),
        })
    }

    /// Load the configured file into memory. Required to create a new file enrichment table.
    pub fn load_file(&self, timezone: TimeZone) -> crate::Result<FileData> {
        match self.file.encoding {
            Encoding::Csv {
                include_headers,
                delimiter,
            } => {
                let mut reader = csv::ReaderBuilder::new()
                    .has_headers(include_headers)
                    .delimiter(delimiter as u8)
                    .from_path(&self.file.path)?;

                let first_row = reader.records().next();
                let headers = if include_headers {
                    reader
                        .headers()?
                        .iter()
                        .map(|col| col.to_string())
                        .collect::<Vec<_>>()
                } else {
                    // If there are no headers in the datafile we make headers as the numerical index of
                    // the column.
                    match first_row {
                        Some(Ok(ref row)) => (0..row.len()).map(|idx| idx.to_string()).collect(),
                        _ => Vec::new(),
                    }
                };

                let data = first_row
                    .into_iter()
                    .chain(reader.records())
                    .map(|row| {
                        Ok(row?
                            .iter()
                            .enumerate()
                            .map(|(idx, col)| self.parse_column(timezone, &headers[idx], idx, col))
                            .collect::<Result<Vec<_>, String>>()?)
                    })
                    .collect::<crate::Result<Vec<_>>>()?;

                trace!(
                    "Loaded enrichment file {} with headers {:?}.",
                    self.file.path.to_str().unwrap_or("path with invalid utf"),
                    headers
                );

                let file = reader.into_inner();

                Ok(FileData {
                    headers,
                    data,
                    modified: file.metadata()?.modified()?,
                })
            }

            Encoding::Json => {
                let contents = fs::read_to_string(&self.file.path)?;
                let modified = fs::metadata(&self.file.path)?.modified()?;
                let json: serde_json::Value = serde_json::from_str(&contents)?;

                // Headers are the top-level JSON object keys; values in each row
                // map positionally to their corresponding header.
                let (headers, data) = match json {
                    serde_json::Value::Object(map) => {
                        let headers: Vec<String> = map.keys().cloned().collect();
                        if headers.is_empty() {
                            return Ok(FileData {
                                headers: vec![],
                                data: vec![],
                                modified,
                            });
                        }
                        let row: Vec<Value> =
                            headers.iter().map(|k| json_to_vrl_value(&map[k])).collect();
                        (headers, vec![row])
                    }
                    serde_json::Value::Array(arr) => {
                        if arr.is_empty() {
                            return Ok(FileData {
                                headers: vec![],
                                data: vec![],
                                modified,
                            });
                        }
                        let first = arr[0]
                            .as_object()
                            .ok_or("JSON array elements must be objects")?;
                        let headers: Vec<String> = first.keys().cloned().collect();
                        let data = arr
                            .iter()
                            .map(|item| {
                                let obj = item
                                    .as_object()
                                    .ok_or("JSON array elements must be objects")?;
                                Ok(headers
                                    .iter()
                                    .map(|k| {
                                        json_to_vrl_value(
                                            obj.get(k).unwrap_or(&serde_json::Value::Null),
                                        )
                                    })
                                    .collect())
                            })
                            .collect::<Result<Vec<Vec<Value>>, &str>>()
                            .map_err(|e| e.to_string())?;
                        (headers, data)
                    }
                    _ => return Err(
                        "JSON enrichment table must contain a top-level object or array of objects"
                            .into(),
                    ),
                };

                trace!(
                    "Loaded JSON enrichment file {} with headers {:?}.",
                    self.file.path.to_str().unwrap_or("path with invalid utf"),
                    headers
                );

                Ok(FileData {
                    headers,
                    data,
                    modified,
                })
            }
        }
    }
}

impl EnrichmentTableConfig for FileConfig {
    async fn build(
        &self,
        globals: &crate::config::GlobalOptions,
    ) -> crate::Result<Box<dyn Table + Send + Sync>> {
        Ok(Box::new(File::new(
            self.clone(),
            self.load_file(globals.timezone())?,
        )))
    }
}

impl_generate_config_from_default!(FileConfig);

/// The data resulting from loading a configured file.
pub struct FileData {
    /// The ordered set of headers of the data columns.
    pub headers: Vec<String>,
    /// The data contained in the file.
    pub data: Vec<Vec<Value>>,
    /// The last modified time of the file.
    pub modified: SystemTime,
}

/// A struct that implements [vector_lib::enrichment::Table] to handle loading enrichment data from a CSV file.
#[derive(Clone)]
pub struct File {
    config: FileConfig,
    last_modified: SystemTime,
    data: IndexedData,
}

impl File {
    /// Creates a new [File] based on the provided config.
    pub fn new(config: FileConfig, data: FileData) -> Self {
        Self {
            config,
            last_modified: data.modified,
            data: IndexedData::new(data.headers, data.data),
        }
    }
}

impl Table for File {
    fn find_table_row<'a>(
        &self,
        case: Case,
        condition: &'a [Condition<'a>],
        select: Option<&'a [String]>,
        wildcard: Option<&Value>,
        index: Option<IndexHandle>,
    ) -> Result<ObjectMap, String> {
        self.data
            .find_table_row(case, condition, select, wildcard, index)
    }

    fn find_table_rows<'a>(
        &self,
        case: Case,
        condition: &'a [Condition<'a>],
        select: Option<&'a [String]>,
        wildcard: Option<&Value>,
        index: Option<IndexHandle>,
    ) -> Result<Vec<ObjectMap>, String> {
        self.data
            .find_table_rows(case, condition, select, wildcard, index)
    }

    fn add_index(&mut self, case: Case, fields: &[&str]) -> Result<IndexHandle, String> {
        self.data.add_index(case, fields)
    }

    /// Returns a list of the field names that are in each index
    fn index_fields(&self) -> Vec<(Case, Vec<String>)> {
        self.data.index_fields()
    }

    /// Checks the modified timestamp of the data file to see if data has changed.
    fn needs_reload(&self) -> bool {
        matches!(fs::metadata(&self.config.file.path)
            .and_then(|metadata| metadata.modified()),
            Ok(modified) if modified > self.last_modified)
    }
}

impl std::fmt::Debug for File {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "File {} row(s) {} index(es)",
            self.data.len(),
            self.data.index_count()
        )
    }
}

#[cfg(test)]
mod tests {
    use std::hash::Hasher;

    use chrono::{TimeZone, Timelike};

    use super::*;

    #[test]
    fn parse_file_with_headers() {
        let dir = tempfile::tempdir().expect("Unable to create tempdir for enrichment table");
        let path = dir.path().join("table.csv");
        fs::write(path.clone(), "foo,bar\na,1\nb,2").expect("Failed to write enrichment table");

        let config = FileConfig {
            file: FileSettings {
                path,
                encoding: Encoding::Csv {
                    include_headers: true,
                    delimiter: default_delimiter(),
                },
            },
            schema: HashMap::new(),
        };
        let data = config
            .load_file(Default::default())
            .expect("Failed to parse csv");
        assert_eq!(vec!["foo".to_string(), "bar".to_string()], data.headers);
        assert_eq!(
            vec![
                vec![Value::from("a"), Value::from("1")],
                vec![Value::from("b"), Value::from("2")],
            ],
            data.data
        );
    }

    #[test]
    fn parse_file_no_headers() {
        let dir = tempfile::tempdir().expect("Unable to create tempdir for enrichment table");
        let path = dir.path().join("table.csv");
        fs::write(path.clone(), "a,1\nb,2").expect("Failed to write enrichment table");

        let config = FileConfig {
            file: FileSettings {
                path,
                encoding: Encoding::Csv {
                    include_headers: false,
                    delimiter: default_delimiter(),
                },
            },
            schema: HashMap::new(),
        };
        let data = config
            .load_file(Default::default())
            .expect("Failed to parse csv");
        assert_eq!(vec!["0".to_string(), "1".to_string()], data.headers);
        assert_eq!(
            vec![
                vec![Value::from("a"), Value::from("1")],
                vec![Value::from("b"), Value::from("2")],
            ],
            data.data
        );
    }

    #[test]
    fn parse_column() {
        let mut schema = HashMap::new();
        schema.insert("col1".to_string(), " string ".to_string());
        schema.insert("col2".to_string(), " date ".to_string());
        schema.insert("col3".to_string(), "date|%m/%d/%Y".to_string());
        schema.insert("col3-spaces".to_string(), "date | %m %d %Y".to_string());
        schema.insert("col4".to_string(), "timestamp|%+".to_string());
        schema.insert("col4-spaces".to_string(), "timestamp | %+".to_string());
        schema.insert("col5".to_string(), "int".to_string());
        let config = FileConfig {
            file: Default::default(),
            schema,
        };

        assert_eq!(
            Ok(Value::from("zork")),
            config.parse_column(Default::default(), "col1", 1, "zork")
        );

        assert_eq!(
            Ok(Value::from(
                chrono::Utc
                    .with_ymd_and_hms(2020, 3, 5, 0, 0, 0)
                    .single()
                    .expect("invalid timestamp")
            )),
            config.parse_column(Default::default(), "col2", 1, "2020-03-05")
        );

        assert_eq!(
            Ok(Value::from(
                chrono::Utc
                    .with_ymd_and_hms(2020, 3, 5, 0, 0, 0)
                    .single()
                    .expect("invalid timestamp")
            )),
            config.parse_column(Default::default(), "col3", 1, "03/05/2020")
        );

        assert_eq!(
            Ok(Value::from(
                chrono::Utc
                    .with_ymd_and_hms(2020, 3, 5, 0, 0, 0)
                    .single()
                    .expect("invalid timestamp")
            )),
            config.parse_column(Default::default(), "col3-spaces", 1, "03 05 2020")
        );

        assert_eq!(
            Ok(Value::from(
                chrono::Utc
                    .with_ymd_and_hms(2001, 7, 7, 15, 4, 0)
                    .single()
                    .and_then(|t| t.with_nanosecond(26490 * 1_000))
                    .expect("invalid timestamp")
            )),
            config.parse_column(
                Default::default(),
                "col4",
                1,
                "2001-07-08T00:34:00.026490+09:30"
            )
        );

        assert_eq!(
            Ok(Value::from(
                chrono::Utc
                    .with_ymd_and_hms(2001, 7, 7, 15, 4, 0)
                    .single()
                    .and_then(|t| t.with_nanosecond(26490 * 1_000))
                    .expect("invalid timestamp")
            )),
            config.parse_column(
                Default::default(),
                "col4-spaces",
                1,
                "2001-07-08T00:34:00.026490+09:30"
            )
        );

        assert_eq!(
            Ok(Value::from(42)),
            config.parse_column(Default::default(), "col5", 1, "42")
        );
    }

    #[test]
    fn seahash() {
        // Ensure we can separate fields to create a distinct hash.
        let mut one = seahash::SeaHasher::default();
        one.write(b"norknoog");
        one.write_u8(0);
        one.write(b"donk");

        let mut two = seahash::SeaHasher::default();
        two.write(b"nork");
        one.write_u8(0);
        two.write(b"noogdonk");

        assert_ne!(one.finish(), two.finish());
    }

    #[test]
    fn finds_row() {
        let file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: vec![
                    vec!["zip".into(), "zup".into()],
                    vec!["zirp".into(), "zurp".into()],
                ],
                headers: vec!["field1".to_string(), "field2".to_string()],
            },
        );

        let condition = Condition::Equals {
            field: "field1",
            value: Value::from("zirp"),
        };

        assert_eq!(
            Ok(ObjectMap::from([
                ("field1".into(), Value::from("zirp")),
                ("field2".into(), Value::from("zurp")),
            ])),
            file.find_table_row(Case::Sensitive, &[condition], None, None, None)
        );
    }

    #[test]
    fn finds_row_with_wildcard() {
        let file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: vec![
                    vec!["zip".into(), "zup".into()],
                    vec!["zirp".into(), "zurp".into()],
                ],
                headers: vec!["field1".to_string(), "field2".to_string()],
            },
        );

        let wildcard = Value::from("zirp");

        let condition = Condition::Equals {
            field: "field1",
            value: Value::from("nonexistent"),
        };

        assert_eq!(
            Ok(ObjectMap::from([
                ("field1".into(), Value::from("zirp")),
                ("field2".into(), Value::from("zurp")),
            ])),
            file.find_table_row(Case::Sensitive, &[condition], None, Some(&wildcard), None)
        );
    }

    #[test]
    fn duplicate_indexes() {
        let mut file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: Vec::new(),
                headers: vec![
                    "field1".to_string(),
                    "field2".to_string(),
                    "field3".to_string(),
                ],
            },
        );

        let handle1 = file.add_index(Case::Sensitive, &["field2", "field3"]);
        let handle2 = file.add_index(Case::Sensitive, &["field3", "field2"]);

        assert_eq!(handle1, handle2);
        assert_eq!(1, file.data.index_count());
    }

    #[test]
    fn errors_on_missing_columns() {
        let mut file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: Vec::new(),
                headers: vec![
                    "field1".to_string(),
                    "field2".to_string(),
                    "field3".to_string(),
                ],
            },
        );

        let error = file.add_index(Case::Sensitive, &["apples", "field2", "bananas"]);
        assert_eq!(
            Err("field(s) 'apples, bananas' missing from dataset".to_string()),
            error
        )
    }

    #[test]
    fn finds_row_with_index() {
        let mut file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: vec![
                    vec!["zip".into(), "zup".into()],
                    vec!["zirp".into(), "zurp".into()],
                ],
                headers: vec!["field1".to_string(), "field2".to_string()],
            },
        );

        let handle = file.add_index(Case::Sensitive, &["field1"]).unwrap();

        let condition = Condition::Equals {
            field: "field1",
            value: Value::from("zirp"),
        };

        assert_eq!(
            Ok(ObjectMap::from([
                ("field1".into(), Value::from("zirp")),
                ("field2".into(), Value::from("zurp")),
            ])),
            file.find_table_row(Case::Sensitive, &[condition], None, None, Some(handle))
        );
    }

    #[test]
    fn finds_row_with_index_case_sensitive_and_wildcard() {
        let mut file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: vec![
                    vec!["zip".into(), "zup".into()],
                    vec!["zirp".into(), "zurp".into()],
                ],
                headers: vec!["field1".to_string(), "field2".to_string()],
            },
        );

        let handle = file.add_index(Case::Sensitive, &["field1"]).unwrap();
        let wildcard = Value::from("zirp");

        let condition = Condition::Equals {
            field: "field1",
            value: Value::from("nonexistent"),
        };

        assert_eq!(
            Ok(ObjectMap::from([
                ("field1".into(), Value::from("zirp")),
                ("field2".into(), Value::from("zurp")),
            ])),
            file.find_table_row(
                Case::Sensitive,
                &[condition],
                None,
                Some(&wildcard),
                Some(handle)
            )
        );
    }

    #[test]
    fn finds_rows_with_index_case_sensitive() {
        let mut file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: vec![
                    vec!["zip".into(), "zup".into()],
                    vec!["zirp".into(), "zurp".into()],
                    vec!["zip".into(), "zoop".into()],
                ],
                headers: vec!["field1".to_string(), "field2".to_string()],
            },
        );

        let handle = file.add_index(Case::Sensitive, &["field1"]).unwrap();

        assert_eq!(
            Ok(vec![
                ObjectMap::from([
                    ("field1".into(), Value::from("zip")),
                    ("field2".into(), Value::from("zup")),
                ]),
                ObjectMap::from([
                    ("field1".into(), Value::from("zip")),
                    ("field2".into(), Value::from("zoop")),
                ]),
            ]),
            file.find_table_rows(
                Case::Sensitive,
                &[Condition::Equals {
                    field: "field1",
                    value: Value::from("zip"),
                }],
                None,
                None,
                Some(handle)
            )
        );

        assert_eq!(
            Ok(vec![]),
            file.find_table_rows(
                Case::Sensitive,
                &[Condition::Equals {
                    field: "field1",
                    value: Value::from("ZiP"),
                }],
                None,
                None,
                Some(handle)
            )
        );
    }

    #[test]
    fn selects_columns() {
        let mut file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: vec![
                    vec!["zip".into(), "zup".into(), "zoop".into()],
                    vec!["zirp".into(), "zurp".into(), "zork".into()],
                    vec!["zip".into(), "zoop".into(), "zibble".into()],
                ],
                headers: vec![
                    "field1".to_string(),
                    "field2".to_string(),
                    "field3".to_string(),
                ],
            },
        );

        let handle = file.add_index(Case::Sensitive, &["field1"]).unwrap();

        let condition = Condition::Equals {
            field: "field1",
            value: Value::from("zip"),
        };

        assert_eq!(
            Ok(vec![
                ObjectMap::from([
                    ("field1".into(), Value::from("zip")),
                    ("field3".into(), Value::from("zoop")),
                ]),
                ObjectMap::from([
                    ("field1".into(), Value::from("zip")),
                    ("field3".into(), Value::from("zibble")),
                ]),
            ]),
            file.find_table_rows(
                Case::Sensitive,
                &[condition],
                Some(&["field1".to_string(), "field3".to_string()]),
                None,
                Some(handle)
            )
        );
    }

    #[test]
    fn finds_rows_with_index_case_insensitive() {
        let mut file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: vec![
                    vec!["zip".into(), "zup".into()],
                    vec!["zirp".into(), "zurp".into()],
                    vec!["zip".into(), "zoop".into()],
                ],
                headers: vec!["field1".to_string(), "field2".to_string()],
            },
        );

        let handle = file.add_index(Case::Insensitive, &["field1"]).unwrap();

        assert_eq!(
            Ok(vec![
                ObjectMap::from([
                    ("field1".into(), Value::from("zip")),
                    ("field2".into(), Value::from("zup")),
                ]),
                ObjectMap::from([
                    ("field1".into(), Value::from("zip")),
                    ("field2".into(), Value::from("zoop")),
                ]),
            ]),
            file.find_table_rows(
                Case::Insensitive,
                &[Condition::Equals {
                    field: "field1",
                    value: Value::from("zip"),
                }],
                None,
                None,
                Some(handle)
            )
        );

        assert_eq!(
            Ok(vec![
                ObjectMap::from([
                    ("field1".into(), Value::from("zip")),
                    ("field2".into(), Value::from("zup")),
                ]),
                ObjectMap::from([
                    ("field1".into(), Value::from("zip")),
                    ("field2".into(), Value::from("zoop")),
                ]),
            ]),
            file.find_table_rows(
                Case::Insensitive,
                &[Condition::Equals {
                    field: "field1",
                    value: Value::from("ZiP"),
                }],
                None,
                None,
                Some(handle)
            )
        );
    }

    #[test]
    fn finds_rows_with_index_case_insensitive_and_wildcard() {
        let mut file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: vec![
                    vec!["zip".into(), "zup".into()],
                    vec!["zirp".into(), "zurp".into()],
                    vec!["zip".into(), "zoop".into()],
                ],
                headers: vec!["field1".to_string(), "field2".to_string()],
            },
        );

        let handle = file.add_index(Case::Insensitive, &["field1"]).unwrap();

        assert_eq!(
            Ok(vec![
                ObjectMap::from([
                    ("field1".into(), Value::from("zip")),
                    ("field2".into(), Value::from("zup")),
                ]),
                ObjectMap::from([
                    ("field1".into(), Value::from("zip")),
                    ("field2".into(), Value::from("zoop")),
                ]),
            ]),
            file.find_table_rows(
                Case::Insensitive,
                &[Condition::Equals {
                    field: "field1",
                    value: Value::from("nonexistent"),
                }],
                None,
                Some(&Value::from("zip")),
                Some(handle)
            )
        );

        assert_eq!(
            Ok(vec![
                ObjectMap::from([
                    ("field1".into(), Value::from("zip")),
                    ("field2".into(), Value::from("zup")),
                ]),
                ObjectMap::from([
                    ("field1".into(), Value::from("zip")),
                    ("field2".into(), Value::from("zoop")),
                ]),
            ]),
            file.find_table_rows(
                Case::Insensitive,
                &[Condition::Equals {
                    field: "field1",
                    value: Value::from("ZiP"),
                }],
                None,
                Some(&Value::from("ZiP")),
                Some(handle)
            )
        );
    }

    #[test]
    fn finds_row_between_dates() {
        let mut file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: vec![
                    vec![
                        "zip".into(),
                        Value::Timestamp(
                            chrono::Utc
                                .with_ymd_and_hms(2015, 12, 7, 0, 0, 0)
                                .single()
                                .expect("invalid timestamp"),
                        ),
                    ],
                    vec![
                        "zip".into(),
                        Value::Timestamp(
                            chrono::Utc
                                .with_ymd_and_hms(2016, 12, 7, 0, 0, 0)
                                .single()
                                .expect("invalid timestamp"),
                        ),
                    ],
                ],
                headers: vec!["field1".to_string(), "field2".to_string()],
            },
        );

        let handle = file.add_index(Case::Sensitive, &["field1"]).unwrap();

        let conditions = [
            Condition::Equals {
                field: "field1",
                value: "zip".into(),
            },
            Condition::BetweenDates {
                field: "field2",
                from: chrono::Utc
                    .with_ymd_and_hms(2016, 1, 1, 0, 0, 0)
                    .single()
                    .expect("invalid timestamp"),
                to: chrono::Utc
                    .with_ymd_and_hms(2017, 1, 1, 0, 0, 0)
                    .single()
                    .expect("invalid timestamp"),
            },
        ];

        assert_eq!(
            Ok(ObjectMap::from([
                ("field1".into(), Value::from("zip")),
                (
                    "field2".into(),
                    Value::Timestamp(
                        chrono::Utc
                            .with_ymd_and_hms(2016, 12, 7, 0, 0, 0)
                            .single()
                            .expect("invalid timestamp")
                    )
                )
            ])),
            file.find_table_row(Case::Sensitive, &conditions, None, None, Some(handle))
        );
    }

    #[test]
    fn finds_row_from_date() {
        let mut file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: vec![
                    vec![
                        "zip".into(),
                        Value::Timestamp(
                            chrono::Utc
                                .with_ymd_and_hms(2015, 12, 7, 0, 0, 0)
                                .single()
                                .expect("invalid timestamp"),
                        ),
                    ],
                    vec![
                        "zip".into(),
                        Value::Timestamp(
                            chrono::Utc
                                .with_ymd_and_hms(2016, 12, 7, 0, 0, 0)
                                .single()
                                .expect("invalid timestamp"),
                        ),
                    ],
                ],
                headers: vec!["field1".to_string(), "field2".to_string()],
            },
        );

        let handle = file.add_index(Case::Sensitive, &["field1"]).unwrap();

        let conditions = [
            Condition::Equals {
                field: "field1",
                value: "zip".into(),
            },
            Condition::FromDate {
                field: "field2",
                from: chrono::Utc
                    .with_ymd_and_hms(2016, 1, 1, 0, 0, 0)
                    .single()
                    .expect("invalid timestamp"),
            },
        ];

        assert_eq!(
            Ok(ObjectMap::from([
                ("field1".into(), Value::from("zip")),
                (
                    "field2".into(),
                    Value::Timestamp(
                        chrono::Utc
                            .with_ymd_and_hms(2016, 12, 7, 0, 0, 0)
                            .single()
                            .expect("invalid timestamp")
                    )
                )
            ])),
            file.find_table_row(Case::Sensitive, &conditions, None, None, Some(handle))
        );
    }

    #[test]
    fn finds_row_to_date() {
        let mut file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: vec![
                    vec![
                        "zip".into(),
                        Value::Timestamp(
                            chrono::Utc
                                .with_ymd_and_hms(2015, 12, 7, 0, 0, 0)
                                .single()
                                .expect("invalid timestamp"),
                        ),
                    ],
                    vec![
                        "zip".into(),
                        Value::Timestamp(
                            chrono::Utc
                                .with_ymd_and_hms(2016, 12, 7, 0, 0, 0)
                                .single()
                                .expect("invalid timestamp"),
                        ),
                    ],
                ],
                headers: vec!["field1".to_string(), "field2".to_string()],
            },
        );

        let handle = file.add_index(Case::Sensitive, &["field1"]).unwrap();

        let conditions = [
            Condition::Equals {
                field: "field1",
                value: "zip".into(),
            },
            Condition::ToDate {
                field: "field2",
                to: chrono::Utc
                    .with_ymd_and_hms(2016, 1, 1, 0, 0, 0)
                    .single()
                    .expect("invalid timestamp"),
            },
        ];

        assert_eq!(
            Ok(ObjectMap::from([
                ("field1".into(), Value::from("zip")),
                (
                    "field2".into(),
                    Value::Timestamp(
                        chrono::Utc
                            .with_ymd_and_hms(2015, 12, 7, 0, 0, 0)
                            .single()
                            .expect("invalid timestamp")
                    )
                )
            ])),
            file.find_table_row(Case::Sensitive, &conditions, None, None, Some(handle))
        );
    }

    #[test]
    fn doesnt_find_row() {
        let file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: vec![
                    vec!["zip".into(), "zup".into()],
                    vec!["zirp".into(), "zurp".into()],
                ],
                headers: vec!["field1".to_string(), "field2".to_string()],
            },
        );

        let condition = Condition::Equals {
            field: "field1",
            value: Value::from("zorp"),
        };

        assert_eq!(
            Err("no rows found".to_string()),
            file.find_table_row(Case::Sensitive, &[condition], None, None, None)
        );
    }

    #[test]
    fn doesnt_find_row_with_index() {
        let mut file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: vec![
                    vec!["zip".into(), "zup".into()],
                    vec!["zirp".into(), "zurp".into()],
                ],
                headers: vec!["field1".to_string(), "field2".to_string()],
            },
        );

        let handle = file.add_index(Case::Sensitive, &["field1"]).unwrap();

        let condition = Condition::Equals {
            field: "field1",
            value: Value::from("zorp"),
        };

        assert_eq!(
            Err("no rows found in index".to_string()),
            file.find_table_row(Case::Sensitive, &[condition], None, None, Some(handle))
        );
    }

    #[test]
    fn doesnt_find_row_with_index_and_wildcard() {
        let mut file = File::new(
            Default::default(),
            FileData {
                modified: SystemTime::now(),
                data: vec![
                    vec!["zip".into(), "zup".into()],
                    vec!["zirp".into(), "zurp".into()],
                ],
                headers: vec!["field1".to_string(), "field2".to_string()],
            },
        );

        let handle = file.add_index(Case::Sensitive, &["field1"]).unwrap();
        let wildcard = Value::from("nonexistent");

        let condition = Condition::Equals {
            field: "field1",
            value: Value::from("zorp"),
        };

        assert_eq!(
            Err("no rows found in index".to_string()),
            file.find_table_row(
                Case::Sensitive,
                &[condition],
                None,
                Some(&wildcard),
                Some(handle)
            )
        );
    }

    // JSON tests

    #[test]
    fn parse_json_object() {
        let dir = tempfile::tempdir().expect("Unable to create tempdir for enrichment table");
        let path = dir.path().join("table.json");
        fs::write(
            path.clone(),
            r#"{"a_string":"hello","an_int":42,"a_float":3.14,"a_bool":true,"a_null":null,"an_array":[1,2],"an_object":{"k":"v"}}"#,
        )
        .expect("Failed to write enrichment table");

        let config = FileConfig {
            file: FileSettings {
                path,
                encoding: Encoding::Json,
            },
            schema: HashMap::new(),
        };
        let data = config
            .load_file(Default::default())
            .expect("Failed to parse json");

        assert_eq!(
            vec![
                "a_string".to_string(),
                "an_int".to_string(),
                "a_float".to_string(),
                "a_bool".to_string(),
                "a_null".to_string(),
                "an_array".to_string(),
                "an_object".to_string(),
            ],
            data.headers
        );
        assert_eq!(
            vec![
                Value::from("hello"),
                Value::Integer(42),
                Value::Float(ordered_float::NotNan::new(3.14).unwrap()),
                Value::Boolean(true),
                Value::Null,
                Value::from("[1,2]"),         // arrays serialized to JSON string
                Value::from("{\"k\":\"v\"}"), // objects serialized to JSON string
            ],
            data.data[0]
        );
    }

    #[test]
    fn parse_json_empty_object() {
        let dir = tempfile::tempdir().expect("Unable to create tempdir for enrichment table");
        let path = dir.path().join("table.json");
        fs::write(path.clone(), "{}").expect("Failed to write enrichment table");

        let config = FileConfig {
            file: FileSettings {
                path,
                encoding: Encoding::Json,
            },
            schema: HashMap::new(),
        };
        let data = config
            .load_file(Default::default())
            .expect("Failed to parse json");

        assert!(data.headers.is_empty());
        assert!(data.data.is_empty());
    }

    #[test]
    fn parse_json_array_of_objects() {
        let dir = tempfile::tempdir().expect("Unable to create tempdir for enrichment table");
        let path = dir.path().join("table.json");
        fs::write(
            path.clone(),
            r#"[{"id":"a","value":"1"},{"id":"b","value":"2"}]"#,
        )
        .expect("Failed to write enrichment table");

        let config = FileConfig {
            file: FileSettings {
                path,
                encoding: Encoding::Json,
            },
            schema: HashMap::new(),
        };
        let data = config
            .load_file(Default::default())
            .expect("Failed to parse json");

        assert_eq!(vec!["id".to_string(), "value".to_string()], data.headers);
        assert_eq!(
            vec![
                vec![Value::from("a"), Value::from("1")],
                vec![Value::from("b"), Value::from("2")],
            ],
            data.data
        );
    }

    #[test]
    fn parse_json_array_missing_keys_default_to_null() {
        // Keys present in the first object but missing in subsequent objects default to null.
        let dir = tempfile::tempdir().expect("Unable to create tempdir for enrichment table");
        let path = dir.path().join("table.json");
        fs::write(path.clone(), r#"[{"a":"1","b":"2"},{"a":"3"}]"#)
            .expect("Failed to write enrichment table");

        let config = FileConfig {
            file: FileSettings {
                path,
                encoding: Encoding::Json,
            },
            schema: HashMap::new(),
        };
        let data = config
            .load_file(Default::default())
            .expect("Failed to parse json");

        assert_eq!(vec!["a".to_string(), "b".to_string()], data.headers);
        assert_eq!(
            vec![
                vec![Value::from("1"), Value::from("2")],
                vec![Value::from("3"), Value::Null],
            ],
            data.data
        );
    }

    #[test]
    fn parse_json_invalid_top_level_type() {
        let dir = tempfile::tempdir().expect("Unable to create tempdir for enrichment table");

        for invalid in [r#""just a string""#, "42"] {
            let path = dir.path().join("table.json");
            fs::write(path.clone(), invalid).expect("Failed to write enrichment table");

            let config = FileConfig {
                file: FileSettings {
                    path,
                    encoding: Encoding::Json,
                },
                schema: HashMap::new(),
            };

            assert!(config.load_file(Default::default()).is_err());
        }
    }
}
