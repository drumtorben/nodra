//! Job files: settings, the sink call and the query with its source calls.
//!
//! ```sql
//! SET nodra.trigger = '30s';
//! INSERT INTO delta('/lake/out', partition_by => 'day')
//! SELECT ... FROM delta('/lake/events') e JOIN delta('/lake/dim') d ON ...;
//! ```
//!
//! Source calls are replaced by registered table names (`__nodra_s0`, `__nodra_s1`, …) so that
//! the rewritten query can be planned by DataFusion like any other SQL.

use std::collections::BTreeMap;
use std::ops::ControlFlow;
use std::path::Path;

use datafusion::sql::sqlparser::ast::{
    Expr, FunctionArg, FunctionArgExpr, Ident, ObjectName, Statement, TableFactor, UnaryOperator,
    Value, VisitMut, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

use crate::error::{NodraError, Result};
use crate::settings::Settings;

/// A parsed job.
#[derive(Debug, Clone)]
pub struct Job {
    /// Job name (progress keys depend on it).
    pub name: String,
    /// Settings from `SET` statements.
    pub settings: Settings,
    /// Where results go.
    pub sink: SinkSpec,
    /// Sources in order of appearance; `sources[i]` is registered as `source_table(i)`.
    pub sources: Vec<SourceSpec>,
    /// The query with source calls replaced by registered table names.
    pub query: String,
    /// The query as written.
    pub original_query: String,
}

/// Name under which source `i` is registered.
pub fn source_table(i: usize) -> String {
    format!("__nodra_s{i}")
}

/// A literal argument of a source or sink call.
#[derive(Debug, Clone, PartialEq)]
pub enum Lit {
    /// String literal.
    Str(String),
    /// Numeric literal, kept as written.
    Num(String),
    /// Boolean literal.
    Bool(bool),
}

impl Lit {
    fn describe(&self) -> String {
        match self {
            Lit::Str(s) => format!("'{s}'"),
            Lit::Num(n) => n.clone(),
            Lit::Bool(b) => b.to_string(),
        }
    }
}

/// A call such as `delta('/x', version => 3)`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Call {
    /// Function name, lower case.
    pub function: String,
    /// Positional arguments.
    pub positional: Vec<Lit>,
    /// Named arguments (`name => value`), names lower case.
    pub named: BTreeMap<String, Lit>,
}

impl Call {
    fn str_pos(&self, i: usize, what: &str) -> Result<String> {
        match self.positional.get(i) {
            Some(Lit::Str(s)) => Ok(s.clone()),
            Some(other) => Err(NodraError::job(format!(
                "{}(): {what} must be a string, got {}",
                self.function,
                other.describe()
            ))),
            None => Err(NodraError::job(format!("{}(): missing {what}", self.function))),
        }
    }

    fn opt_str(&self, name: &str) -> Result<Option<String>> {
        match self.named.get(name) {
            None => Ok(None),
            Some(Lit::Str(s)) => Ok(Some(s.clone())),
            Some(other) => Err(NodraError::job(format!(
                "{}(): `{name}` must be a string, got {}",
                self.function,
                other.describe()
            ))),
        }
    }

    fn opt_bool(&self, name: &str) -> Result<Option<bool>> {
        match self.named.get(name) {
            None => Ok(None),
            Some(Lit::Bool(b)) => Ok(Some(*b)),
            Some(Lit::Str(s)) if s.eq_ignore_ascii_case("true") => Ok(Some(true)),
            Some(Lit::Str(s)) if s.eq_ignore_ascii_case("false") => Ok(Some(false)),
            Some(other) => Err(NodraError::job(format!(
                "{}(): `{name}` must be a boolean, got {}",
                self.function,
                other.describe()
            ))),
        }
    }

    fn opt_i64(&self, name: &str) -> Result<Option<i64>> {
        match self.named.get(name) {
            None => Ok(None),
            Some(Lit::Num(n)) => n.parse().map(Some).map_err(|_| {
                NodraError::job(format!("{}(): `{name}` must be an integer", self.function))
            }),
            Some(other) => Err(NodraError::job(format!(
                "{}(): `{name}` must be an integer, got {}",
                self.function,
                other.describe()
            ))),
        }
    }

    fn check_args(&self, max_positional: usize, allowed: &[&str], allow_dotted: bool) -> Result<()> {
        if self.positional.len() > max_positional {
            return Err(NodraError::job(format!(
                "{}() takes at most {max_positional} positional argument(s)",
                self.function
            )));
        }
        for k in self.named.keys() {
            if !allowed.contains(&k.as_str()) && !(allow_dotted && k.contains('.')) {
                return Err(NodraError::job(format!(
                    "{}(): unknown argument `{k}` (allowed: {})",
                    self.function,
                    allowed.join(", ")
                )));
            }
        }
        Ok(())
    }
}

/// Message format of a stream source.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamFormat {
    /// One JSON object per message.
    Json,
}

/// A source referenced in the query.
#[derive(Debug, Clone, PartialEq)]
pub enum SourceSpec {
    /// `delta('<uri>' [, version => n] [, broadcast => bool])`
    Delta {
        /// Table URI or local path.
        uri: String,
        /// Pin the table to a version (batch only).
        version: Option<i64>,
        /// Force (true) or forbid (false) broadcasting; default is size based.
        broadcast: Option<bool>,
    },
    /// `kafka('<topic>', brokers => '…', schema => '…' [, format => 'json'] [, "<librdkafka.prop>" => '…'])`
    Kafka {
        /// Topic name.
        topic: String,
        /// Bootstrap servers.
        brokers: String,
        /// Message format.
        format: StreamFormat,
        /// Column definitions, e.g. `ts TIMESTAMP, id BIGINT`.
        schema: String,
        /// Extra librdkafka properties.
        properties: BTreeMap<String, String>,
    },
    /// `jsonl_stream('<dir>', schema => '…')`: a directory of `<partition>.jsonl` files, one
    /// record per line, offset = line number. A broker-free stand-in for Kafka (tests, demos).
    JsonlStream {
        /// Directory containing `0.jsonl`, `1.jsonl`, ….
        dir: String,
        /// Column definitions.
        schema: String,
    },
}

impl SourceSpec {
    /// True for unbounded sources (Kafka, jsonl_stream).
    pub fn is_stream(&self) -> bool {
        !matches!(self, SourceSpec::Delta { .. })
    }

    /// Stable identifier used in progress keys.
    pub fn key(&self) -> String {
        match self {
            SourceSpec::Delta { uri, .. } => format!("delta:{}", normalize_location(uri)),
            SourceSpec::Kafka { topic, .. } => format!("kafka:{topic}"),
            SourceSpec::JsonlStream { dir, .. } => format!("jsonl:{}", normalize_location(dir)),
        }
    }

    /// Human-readable description for `plan --explain`.
    pub fn describe(&self) -> String {
        match self {
            SourceSpec::Delta { uri, version, .. } => match version {
                Some(v) => format!("delta('{uri}') @ version {v}"),
                None => format!("delta('{uri}')"),
            },
            SourceSpec::Kafka { topic, brokers, .. } => format!("kafka('{topic}') via {brokers}"),
            SourceSpec::JsonlStream { dir, .. } => format!("jsonl_stream('{dir}')"),
        }
    }
}

/// How the sink applies a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkMode {
    /// Add the batch's rows.
    Append,
    /// Replace the table contents (only valid with `--once`).
    Overwrite,
}

/// The sink of the job.
#[derive(Debug, Clone, PartialEq)]
pub enum SinkSpec {
    /// `delta('<uri>' [, partition_by => 'a,b'] [, mode => 'append'|'overwrite'])`
    Delta {
        /// Table URI or local path.
        uri: String,
        /// Partition columns (must be output columns).
        partition_by: Vec<String>,
        /// Write mode.
        mode: SinkMode,
    },
}

impl SinkSpec {
    /// The sink URI.
    pub fn uri(&self) -> &str {
        match self {
            SinkSpec::Delta { uri, .. } => uri,
        }
    }
    /// Partition columns.
    pub fn partition_by(&self) -> &[String] {
        match self {
            SinkSpec::Delta { partition_by, .. } => partition_by,
        }
    }
}

impl Job {
    /// Parse a job file from disk; the job name defaults to the file stem.
    pub fn from_file(path: &Path) -> Result<Job> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| NodraError::job(format!("cannot read {}: {e}", path.display())))?;
        let stem = path
            .file_stem()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_else(|| "job".to_string());
        Job::parse(&text, &stem)
    }

    /// Parse job text. `default_name` is used unless `nodra.job_name` is set.
    pub fn parse(text: &str, default_name: &str) -> Result<Job> {
        let mut settings = Settings::default();
        let mut insert: Option<String> = None;
        for stmt in split_statements(text)? {
            let lower = stmt.to_ascii_lowercase();
            if lower.starts_with("set ") || lower.starts_with("set\t") || lower.starts_with("set\n") {
                let (k, v) = parse_set(&stmt)?;
                settings.set(&k, &v)?;
            } else if lower.starts_with("insert") {
                if insert.is_some() {
                    return Err(NodraError::job("a job must contain exactly one INSERT INTO statement"));
                }
                insert = Some(stmt);
            } else {
                return Err(NodraError::job(format!(
                    "unsupported statement (only SET and one INSERT INTO are allowed): {}",
                    first_line(&stmt)
                )));
            }
        }
        let insert = insert.ok_or_else(|| NodraError::job("missing INSERT INTO statement"))?;
        let (sink_call, query_text) = split_insert(&insert)?;
        let sink = sink_from_call(&parse_call(&sink_call)?)?;
        let (query, sources) = rewrite_query(&query_text)?;
        if sources.is_empty() {
            return Err(NodraError::job("the query reads no source (use delta(), kafka() or jsonl_stream())"));
        }
        if sources.iter().filter(|s| s.is_stream()).count() > 1 {
            return Err(NodraError::rejected(
                "the query joins two stream sources",
                "stream-stream joins need a shuffle; join a stream with Delta tables instead",
            ));
        }
        let name = settings.get("nodra.job_name").unwrap_or(default_name).to_string();
        validate_job_name(&name)?;
        Ok(Job { name, settings, sink, sources, query, original_query: query_text })
    }

    /// Index of the stream source, if any.
    pub fn stream_source(&self) -> Option<usize> {
        self.sources.iter().position(SourceSpec::is_stream)
    }
}

fn validate_job_name(name: &str) -> Result<()> {
    if name.is_empty()
        || !name.chars().all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
    {
        return Err(NodraError::job(format!(
            "invalid job name `{name}` (use letters, digits, `_`, `-`, `.`)"
        )));
    }
    Ok(())
}

fn first_line(s: &str) -> &str {
    s.lines().next().unwrap_or("").trim()
}

/// Split a script into statements on `;`, honouring quotes and comments. Comments are removed.
fn split_statements(text: &str) -> Result<Vec<String>> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '\'' | '"' | '`' => {
                let q = c;
                cur.push(c);
                i += 1;
                loop {
                    if i >= chars.len() {
                        return Err(NodraError::job("unterminated quoted string"));
                    }
                    cur.push(chars[i]);
                    if chars[i] == q {
                        if i + 1 < chars.len() && chars[i + 1] == q {
                            cur.push(chars[i + 1]);
                            i += 2;
                            continue;
                        }
                        i += 1;
                        break;
                    }
                    i += 1;
                }
            }
            '-' if i + 1 < chars.len() && chars[i + 1] == '-' => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
            }
            '/' if i + 1 < chars.len() && chars[i + 1] == '*' => {
                i += 2;
                while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                    i += 1;
                }
                if i + 1 >= chars.len() {
                    return Err(NodraError::job("unterminated /* comment"));
                }
                i += 2;
                cur.push(' ');
            }
            ';' => {
                if !cur.trim().is_empty() {
                    out.push(cur.trim().to_string());
                }
                cur.clear();
                i += 1;
            }
            _ => {
                cur.push(c);
                i += 1;
            }
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    Ok(out)
}

fn parse_set(stmt: &str) -> Result<(String, String)> {
    let body = stmt[3..].trim();
    let (k, v) = if let Some(pos) = body.find('=') {
        (&body[..pos], &body[pos + 1..])
    } else {
        let lower = body.to_ascii_lowercase();
        let pos = lower
            .find(" to ")
            .ok_or_else(|| NodraError::job(format!("expected `SET key = value`, got `{stmt}`")))?;
        (&body[..pos], &body[pos + 4..])
    };
    let k = k.trim().trim_matches('"').to_string();
    let v = v.trim();
    let v = if v.len() >= 2 && v.starts_with('\'') && v.ends_with('\'') {
        v[1..v.len() - 1].replace("''", "'")
    } else {
        v.to_string()
    };
    if k.is_empty() {
        return Err(NodraError::job(format!("expected `SET key = value`, got `{stmt}`")));
    }
    Ok((k, v))
}

/// Split `INSERT INTO <call> <query>` into the call text and the query text.
fn split_insert(stmt: &str) -> Result<(String, String)> {
    let lower = stmt.to_ascii_lowercase();
    let rest_start = lower
        .strip_prefix("insert")
        .and_then(|r| {
            let r2 = r.trim_start();
            r2.strip_prefix("into").map(|r3| stmt.len() - r3.len())
        })
        .ok_or_else(|| NodraError::job("expected `INSERT INTO <sink>(...) <query>`"))?;
    let rest = &stmt[rest_start..];
    let open = rest
        .find('(')
        .ok_or_else(|| NodraError::job("the sink must be a call such as delta('/path')"))?;
    let name = rest[..open].trim();
    if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
        return Err(NodraError::job(format!(
            "the sink must be a call such as delta('/path'), got `{}`",
            first_line(rest)
        )));
    }
    let close = matching_paren(rest, open)?;
    let call = rest[..=close].trim().to_string();
    let query = rest[close + 1..].trim().to_string();
    if query.is_empty() {
        return Err(NodraError::job("INSERT INTO has no query"));
    }
    Ok((call, query))
}

fn matching_paren(s: &str, open: usize) -> Result<usize> {
    let b = s.as_bytes();
    let mut depth = 0i32;
    let mut i = open;
    let mut quote: Option<u8> = None;
    while i < b.len() {
        let c = b[i];
        if let Some(q) = quote {
            if c == q {
                if i + 1 < b.len() && b[i + 1] == q {
                    i += 2;
                    continue;
                }
                quote = None;
            }
        } else if c == b'\'' || c == b'"' {
            quote = Some(c);
        } else if c == b'(' {
            depth += 1;
        } else if c == b')' {
            depth -= 1;
            if depth == 0 {
                return Ok(i);
            }
        }
        i += 1;
    }
    Err(NodraError::job("unbalanced parentheses in sink call"))
}

/// Parse a single call by letting sqlparser parse `SELECT * FROM <call>`.
fn parse_call(call_text: &str) -> Result<Call> {
    let sql = format!("SELECT * FROM {call_text}");
    let stmts = Parser::parse_sql(&GenericDialect {}, &sql)
        .map_err(|e| NodraError::job(format!("cannot parse `{call_text}`: {e}")))?;
    let mut calls = Vec::new();
    let mut err = None;
    let mut stmts = stmts;
    let _ = stmts.visit(&mut CallCollector { calls: &mut calls, err: &mut err });
    if let Some(e) = err {
        return Err(e);
    }
    calls.pop().ok_or_else(|| NodraError::job(format!("`{call_text}` is not a call")))
}

struct CallCollector<'a> {
    calls: &'a mut Vec<Call>,
    err: &'a mut Option<NodraError>,
}

impl VisitorMut for CallCollector<'_> {
    type Break = ();
    fn pre_visit_table_factor(&mut self, tf: &mut TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table { name, args: Some(args), .. } = tf {
            match call_from_args(name, &args.args) {
                Ok(c) => self.calls.push(c),
                Err(e) => {
                    *self.err = Some(e);
                    return ControlFlow::Break(());
                }
            }
        }
        ControlFlow::Continue(())
    }
}

fn call_from_args(name: &ObjectName, args: &[FunctionArg]) -> Result<Call> {
    let function = name.to_string().to_ascii_lowercase();
    let mut call = Call { function: function.clone(), ..Default::default() };
    for a in args {
        match a {
            FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                if !call.named.is_empty() {
                    return Err(NodraError::job(format!(
                        "{function}(): positional arguments must come before named ones"
                    )));
                }
                call.positional.push(lit(e, &function)?);
            }
            FunctionArg::Named { name, arg: FunctionArgExpr::Expr(e), .. } => {
                let key = name.value.to_ascii_lowercase();
                if call.named.insert(key.clone(), lit(e, &function)?).is_some() {
                    return Err(NodraError::job(format!("{function}(): duplicate argument `{key}`")));
                }
            }
            other => {
                return Err(NodraError::job(format!("{function}(): unsupported argument `{other}`")))
            }
        }
    }
    Ok(call)
}

fn lit(e: &Expr, function: &str) -> Result<Lit> {
    match e {
        Expr::Value(v) => match &v.value {
            Value::SingleQuotedString(s) | Value::DoubleQuotedString(s) => Ok(Lit::Str(s.clone())),
            Value::Number(n, _) => Ok(Lit::Num(n.to_string())),
            Value::Boolean(b) => Ok(Lit::Bool(*b)),
            other => Err(NodraError::job(format!("{function}(): unsupported literal `{other}`"))),
        },
        Expr::UnaryOp { op: UnaryOperator::Minus, expr } => match lit(expr, function)? {
            Lit::Num(n) => Ok(Lit::Num(format!("-{n}"))),
            _ => Err(NodraError::job(format!("{function}(): unsupported argument `{e}`"))),
        },
        Expr::Identifier(id) if id.value.eq_ignore_ascii_case("true") => Ok(Lit::Bool(true)),
        Expr::Identifier(id) if id.value.eq_ignore_ascii_case("false") => Ok(Lit::Bool(false)),
        _ => Err(NodraError::job(format!(
            "{function}(): arguments must be literals, got `{e}`"
        ))),
    }
}

fn sink_from_call(c: &Call) -> Result<SinkSpec> {
    match c.function.as_str() {
        "delta" => {
            c.check_args(1, &["partition_by", "mode"], false)?;
            let uri = c.str_pos(0, "table location")?;
            let partition_by = c
                .opt_str("partition_by")?
                .map(|s| {
                    s.split(',')
                        .map(|p| p.trim().to_string())
                        .filter(|p| !p.is_empty())
                        .collect()
                })
                .unwrap_or_default();
            let mode = match c.opt_str("mode")?.as_deref().map(str::to_ascii_lowercase).as_deref() {
                None | Some("append") => SinkMode::Append,
                Some("overwrite") => SinkMode::Overwrite,
                Some(m) => {
                    return Err(NodraError::job(format!(
                        "delta(): mode must be 'append' or 'overwrite', got '{m}'"
                    )))
                }
            };
            Ok(SinkSpec::Delta { uri, partition_by, mode })
        }
        other => Err(NodraError::job(format!("unknown sink `{other}` (supported: delta)"))),
    }
}

fn source_from_call(c: &Call) -> Result<SourceSpec> {
    match c.function.as_str() {
        "delta" => {
            c.check_args(1, &["version", "broadcast"], false)?;
            Ok(SourceSpec::Delta {
                uri: c.str_pos(0, "table location")?,
                version: c.opt_i64("version")?,
                broadcast: c.opt_bool("broadcast")?,
            })
        }
        "kafka" => {
            c.check_args(1, &["brokers", "format", "schema"], true)?;
            let format = match c.opt_str("format")?.as_deref().map(str::to_ascii_lowercase).as_deref() {
                None | Some("json") => StreamFormat::Json,
                Some(f) => {
                    return Err(NodraError::job(format!(
                        "kafka(): unsupported format '{f}' (supported: json)"
                    )))
                }
            };
            let properties = c
                .named
                .iter()
                .filter(|(k, _)| k.contains('.'))
                .map(|(k, v)| match v {
                    Lit::Str(s) => Ok((k.clone(), s.clone())),
                    Lit::Num(n) => Ok((k.clone(), n.clone())),
                    Lit::Bool(b) => Ok((k.clone(), b.to_string())),
                })
                .collect::<Result<BTreeMap<_, _>>>()?;
            Ok(SourceSpec::Kafka {
                topic: c.str_pos(0, "topic")?,
                brokers: c
                    .opt_str("brokers")?
                    .ok_or_else(|| NodraError::job("kafka(): `brokers` is required"))?,
                format,
                schema: c
                    .opt_str("schema")?
                    .ok_or_else(|| NodraError::job("kafka(): `schema` is required"))?,
                properties,
            })
        }
        "jsonl_stream" => {
            c.check_args(1, &["schema"], false)?;
            Ok(SourceSpec::JsonlStream {
                dir: c.str_pos(0, "directory")?,
                schema: c
                    .opt_str("schema")?
                    .ok_or_else(|| NodraError::job("jsonl_stream(): `schema` is required"))?,
            })
        }
        other => Err(NodraError::job(format!(
            "unknown table function `{other}` (supported: delta, kafka, jsonl_stream)"
        ))),
    }
}

const SOURCE_FUNCTIONS: &[&str] = &["delta", "kafka", "jsonl_stream"];

struct SourceRewriter {
    sources: Vec<SourceSpec>,
    err: Option<NodraError>,
}

impl VisitorMut for SourceRewriter {
    type Break = ();
    fn pre_visit_table_factor(&mut self, tf: &mut TableFactor) -> ControlFlow<()> {
        if let TableFactor::Table { name, args, .. } = tf {
            let fname = name.to_string().to_ascii_lowercase();
            if let Some(a) = args.as_ref() {
                if !SOURCE_FUNCTIONS.contains(&fname.as_str()) {
                    self.err = Some(NodraError::job(format!(
                        "unknown table function `{fname}` (supported: delta, kafka, jsonl_stream)"
                    )));
                    return ControlFlow::Break(());
                }
                let spec = call_from_args(name, &a.args).and_then(|c| source_from_call(&c));
                match spec {
                    Ok(spec) => {
                        let tname = source_table(self.sources.len());
                        self.sources.push(spec);
                        *name = ObjectName::from(vec![Ident::new(tname)]);
                        *args = None;
                    }
                    Err(e) => {
                        self.err = Some(e);
                        return ControlFlow::Break(());
                    }
                }
            }
        }
        ControlFlow::Continue(())
    }
}

/// Replace every source call in `query` by a registered table name.
fn rewrite_query(query: &str) -> Result<(String, Vec<SourceSpec>)> {
    let mut stmts = Parser::parse_sql(&GenericDialect {}, query)
        .map_err(|e| NodraError::job(format!("cannot parse query: {e}")))?;
    if stmts.len() != 1 || !matches!(stmts[0], Statement::Query(_)) {
        return Err(NodraError::job("INSERT INTO must be followed by a single SELECT/WITH query"));
    }
    let mut rw = SourceRewriter { sources: Vec::new(), err: None };
    let _ = stmts.visit(&mut rw);
    if let Some(e) = rw.err {
        return Err(e);
    }
    Ok((stmts[0].to_string(), rw.sources))
}

/// A canonical spelling of a table location, so that `/x/events`, `/x/./events/` and
/// `file:///x/events` name the same table: URLs are normalized (`.`/`..` segments, no trailing
/// slash), local paths made absolute and normalized lexically (symlinks are not resolved).
pub fn normalize_location(location: &str) -> String {
    if let Ok(url) = url::Url::parse(location) {
        if url.scheme() != "file" && url.scheme().len() > 1 {
            return url.as_str().trim_end_matches('/').to_string();
        }
        if let Ok(path) = url.to_file_path() {
            return normalize_path(&path);
        }
    }
    normalize_path(std::path::Path::new(location))
}

fn normalize_path(path: &std::path::Path) -> String {
    use std::path::Component;
    let absolute = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let mut out = std::path::PathBuf::new();
    for c in absolute.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            other => out.push(other.as_os_str()),
        }
    }
    let s = out.to_string_lossy().to_string();
    if s.len() > 1 { s.trim_end_matches('/').to_string() } else { s }
}

#[cfg(test)]
mod tests {
    use super::*;

    const JOB: &str = r#"
        -- hourly counts
        SET nodra.trigger = '30s';
        SET datafusion.execution.batch_size TO 4096;
        INSERT INTO delta('/lake/out', partition_by => 'day, brand')
        SELECT e.day, c.brand, count(*) AS n /* comment; with semicolon */
        FROM delta('/lake/events') e
        JOIN delta('/lake/dim', broadcast => true) AS c ON e.cid = c.id
        WHERE e.note <> 'a;b'
        GROUP BY 1, 2;
    "#;

    #[test]
    fn parses_job() {
        let job = Job::parse(JOB, "hourly").unwrap();
        assert_eq!(job.name, "hourly");
        assert_eq!(job.settings.get("nodra.trigger"), Some("30s"));
        assert_eq!(job.settings.get("datafusion.execution.batch_size"), Some("4096"));
        assert_eq!(
            job.sink,
            SinkSpec::Delta {
                uri: "/lake/out".into(),
                partition_by: vec!["day".into(), "brand".into()],
                mode: SinkMode::Append
            }
        );
        assert_eq!(job.sources.len(), 2);
        assert_eq!(
            job.sources[1],
            SourceSpec::Delta { uri: "/lake/dim".into(), version: None, broadcast: Some(true) }
        );
        assert!(job.query.contains("__nodra_s0 e"), "{}", job.query);
        assert!(job.query.contains("__nodra_s1 AS c"), "{}", job.query);
        assert!(job.query.contains("'a;b'"));
    }

    #[test]
    fn kafka_source_with_properties() {
        let job = Job::parse(
            "INSERT INTO delta('/o') SELECT * FROM kafka('events', brokers => 'k:9092', \
             schema => 'id BIGINT', \"security.protocol\" => 'SSL')",
            "j",
        )
        .unwrap();
        match &job.sources[0] {
            SourceSpec::Kafka { topic, brokers, properties, .. } => {
                assert_eq!(topic, "events");
                assert_eq!(brokers, "k:9092");
                assert_eq!(properties.get("security.protocol").map(String::as_str), Some("SSL"));
            }
            other => panic!("unexpected {other:?}"),
        }
        assert_eq!(job.stream_source(), Some(0));
    }

    #[test]
    fn rejects_bad_jobs() {
        assert!(Job::parse("SELECT 1", "j").is_err());
        assert!(Job::parse("INSERT INTO delta('/o') SELECT 1", "j").is_err());
        assert!(Job::parse("INSERT INTO nope('/o') SELECT * FROM delta('/x')", "j").is_err());
        assert!(Job::parse("INSERT INTO delta('/o') SELECT * FROM foo('/x')", "j").is_err());
        assert!(Job::parse("SET nodra.bogus = 1; INSERT INTO delta('/o') SELECT * FROM delta('/x')", "j").is_err());
        assert!(Job::parse("INSERT INTO delta('/o', speed => 'fast') SELECT * FROM delta('/x')", "j").is_err());
        let two_streams = "INSERT INTO delta('/o') SELECT * FROM jsonl_stream('/a', schema => 'x INT') a \
                           JOIN jsonl_stream('/b', schema => 'x INT') b ON a.x = b.x";
        assert!(matches!(Job::parse(two_streams, "j"), Err(NodraError::Rejected(_))));
    }

    #[test]
    fn job_name_setting() {
        let job = Job::parse(
            "SET nodra.job_name = 'my-job'; INSERT INTO delta('/o') SELECT * FROM delta('/x')",
            "file",
        )
        .unwrap();
        assert_eq!(job.name, "my-job");
        assert!(Job::parse(
            "SET nodra.job_name = 'bad name'; INSERT INTO delta('/o') SELECT * FROM delta('/x')",
            "f"
        )
        .is_err());
    }

    #[test]
    fn locations_are_normalized() {
        assert_eq!(normalize_location("/x/events"), "/x/events");
        assert_eq!(normalize_location("/x/./events/"), "/x/events");
        assert_eq!(normalize_location("/x/y/../events"), "/x/events");
        assert_eq!(normalize_location("file:///x/events/"), "/x/events");
        assert_eq!(
            normalize_location("abfss://c@a.dfs.core.windows.net/lake/./t/"),
            "abfss://c@a.dfs.core.windows.net/lake/t"
        );
        assert!(normalize_location("rel/events").ends_with("/rel/events"));
    }
}
