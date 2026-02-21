//! List command implementation.
//!
//! Lists repository directory contents with support for long, short, and JSON output formats.

use std::io::{self, Write};
use std::sync::Arc;

use crossterm::style::{Color, Stylize};
use serde::Serialize;
use thiserror::Error;

use crate::app::{App, AppCreateRepoContext};
use crate::cli::CommandContext;
use crate::repo::RepoError;
use crate::repo_model::{CommitModel, DirectoryModel, EntryModel, ResolvePathError, ResolvePathResult};
use crate::repository::ObjectId;

// =============================================================================
// Error Types
// =============================================================================

/// Errors that can occur during the list command.
#[derive(Debug, Error)]
pub enum ListError {
    /// Path not found in repository.
    #[error("path not found: {0}")]
    PathNotFound(String),

    /// Repository error.
    #[error("repository error: {0}")]
    Repo(#[from] RepoError),

    /// Path resolution error.
    #[error("{0}")]
    ResolvePath(#[from] ResolvePathError),

    /// Missing required context field.
    #[error("missing required context: {0}")]
    MissingContext(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// App error.
    #[error("app error: {0}")]
    App(#[from] crate::app::AppError),
}

// =============================================================================
// Command Arguments
// =============================================================================

/// Arguments for the list command.
pub struct ListCommandArgs {
    /// Path within the repository to list.
    pub path: String,
    /// Output format.
    pub format: ListFormat,
    /// Whether to append format indicators (/ for dirs, * for executables).
    pub classify_format: bool,
}

/// Output format for the list command.
pub enum ListFormat {
    /// Long format (one entry per line with details).
    Long(ListLongFormat),
    /// Short format (names only, possibly multi-column).
    Short(ListShortFormat),
    /// JSON format (JSONL).
    Json(ListJsonFormat),
}

/// Long format options.
pub struct ListLongFormat {
    /// Whether to include object IDs in output.
    pub include_id: bool,
}

/// Short format options.
pub struct ListShortFormat {
    /// Terminal width (None = detect automatically).
    pub width: Option<usize>,
    /// Column layout specification.
    pub columns_spec: ListColumnsSpec,
}

/// JSON format options.
pub struct ListJsonFormat {
    /// Whether to include object IDs in output.
    pub include_id: bool,
}

/// Column layout specification for short format.
#[derive(Clone, Copy, Default)]
pub enum ListColumnsSpec {
    /// Force single column.
    Single,
    /// Multiple columns, entries sorted vertically (default, like ls).
    #[default]
    MultiVerticalSort,
    /// Multiple columns, entries sorted horizontally (like ls -x).
    MultiHorizontalSort,
}

// =============================================================================
// JSON Output Types
// =============================================================================

/// JSON output for file entries.
#[derive(Serialize)]
pub struct ListEntryFileJson {
    #[serde(rename = "type")]
    pub type_tag: String,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executable: Option<bool>,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

/// JSON output for directory entries.
#[derive(Serialize)]
pub struct ListEntryDirectoryJson {
    #[serde(rename = "type")]
    pub type_tag: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

// =============================================================================
// Internal Entry Type
// =============================================================================

/// Internal representation of a list entry.
enum ListEntry {
    File {
        name: String,
        size: u64,
        executable: bool,
        id: ObjectId,
    },
    Directory {
        name: String,
        id: ObjectId,
    },
}

impl ListEntry {
    fn name(&self) -> &str {
        match self {
            ListEntry::File { name, .. } => name,
            ListEntry::Directory { name, .. } => name,
        }
    }

    fn is_directory(&self) -> bool {
        matches!(self, ListEntry::Directory { .. })
    }

    fn is_executable(&self) -> bool {
        matches!(self, ListEntry::File { executable: true, .. })
    }
}

// =============================================================================
// Command Implementation
// =============================================================================

/// Execute the list command.
pub async fn list(args: ListCommandArgs, context: &CommandContext, app: &App) -> Result<(), ListError> {
    // Get required context fields
    let repo_spec = context
        .repository_spec
        .as_ref()
        .ok_or_else(|| ListError::MissingContext("repository_spec".to_string()))?;
    let commit_id = context
        .commit
        .as_ref()
        .ok_or_else(|| ListError::MissingContext("commit".to_string()))?;

    // Create repo and get commit's root directory
    let repo = app
        .create_repo(AppCreateRepoContext::new(repo_spec))
        .await?;
    let commit_model = CommitModel::new(Arc::clone(&repo), commit_id.clone());
    let root = commit_model.root().await?;

    // Resolve the path and collect entries
    let entries = match root.resolve_path(&args.path).await? {
        ResolvePathResult::None => {
            return Err(ListError::PathNotFound(args.path));
        }
        ResolvePathResult::Root => {
            collect_directory_entries(&root.directory()).await?
        }
        ResolvePathResult::Directory(dir_entry) => {
            collect_directory_entries(&dir_entry.directory()).await?
        }
        ResolvePathResult::File(file_entry) => {
            // Single file entry
            vec![ListEntry::File {
                name: file_entry.name().to_string(),
                size: file_entry.size(),
                executable: file_entry.executable(),
                id: file_entry.id().clone(),
            }]
        }
    };

    // Output based on format
    match args.format {
        ListFormat::Long(fmt) => format_long(&entries, fmt.include_id, args.classify_format),
        ListFormat::Short(fmt) => format_short(&entries, &fmt, args.classify_format),
        ListFormat::Json(fmt) => format_json(&entries, fmt.include_id),
    }
}

/// Collect all entries from a directory.
async fn collect_directory_entries(dir: &DirectoryModel) -> Result<Vec<ListEntry>, ListError> {
    let mut entries = Vec::new();
    let mut entry_list = dir.entries().await?;

    while let Some(entry) = entry_list.next().await? {
        let list_entry = match entry {
            EntryModel::File(f) => ListEntry::File {
                name: f.name().to_string(),
                size: f.size(),
                executable: f.executable(),
                id: f.id().clone(),
            },
            EntryModel::Directory(d) => ListEntry::Directory {
                name: d.name.clone(),
                id: d.id().clone(),
            },
        };
        entries.push(list_entry);
    }

    Ok(entries)
}

// =============================================================================
// Long Format
// =============================================================================

/// Format entries in long format.
fn format_long(entries: &[ListEntry], include_id: bool, classify: bool) -> Result<(), ListError> {
    // Calculate column widths
    let max_size_width = entries
        .iter()
        .filter_map(|e| match e {
            ListEntry::File { size, .. } => Some(size.to_string().len()),
            ListEntry::Directory { .. } => None,
        })
        .max()
        .unwrap_or(0);

    let stdout = io::stdout();
    let mut handle = stdout.lock();

    for entry in entries {
        let type_char = match entry {
            ListEntry::Directory { .. } => 'd',
            ListEntry::File { executable: true, .. } => 'x',
            ListEntry::File { executable: false, .. } => '-',
        };

        let id_str = if include_id {
            match entry {
                ListEntry::File { id, .. } => format!(" {}", id),
                ListEntry::Directory { id, .. } => format!(" {}", id),
            }
        } else {
            String::new()
        };

        let size_str = match entry {
            ListEntry::File { size, .. } => format!("{:>width$}", size, width = max_size_width),
            ListEntry::Directory { .. } => " ".repeat(max_size_width),
        };

        let formatted_name = format_name(entry, classify);

        writeln!(handle, "{}{} {} {}", type_char, id_str, size_str, formatted_name)?;
    }

    Ok(())
}

// =============================================================================
// Short Format
// =============================================================================

/// Format entries in short format.
fn format_short(entries: &[ListEntry], fmt: &ListShortFormat, classify: bool) -> Result<(), ListError> {
    if entries.is_empty() {
        return Ok(());
    }

    let width = fmt.width.unwrap_or_else(|| {
        crossterm::terminal::size()
            .map(|(w, _)| w as usize)
            .unwrap_or(80)
    });

    let stdout = io::stdout();
    let mut handle = stdout.lock();

    match fmt.columns_spec {
        ListColumnsSpec::Single => {
            for entry in entries {
                writeln!(handle, "{}", format_name(entry, classify))?;
            }
        }
        ListColumnsSpec::MultiVerticalSort => {
            format_multi_column_vertical(entries, width, classify, &mut handle)?;
        }
        ListColumnsSpec::MultiHorizontalSort => {
            format_multi_column_horizontal(entries, width, classify, &mut handle)?;
        }
    }

    Ok(())
}

/// Format entries in multi-column layout with vertical sorting.
fn format_multi_column_vertical<W: Write>(
    entries: &[ListEntry],
    width: usize,
    classify: bool,
    handle: &mut W,
) -> Result<(), ListError> {
    let max_name_len = entries
        .iter()
        .map(|e| display_width(e, classify))
        .max()
        .unwrap_or(0);

    let column_width = max_name_len + 2; // 2 spaces between columns
    let num_columns = std::cmp::max(1, width / column_width);
    let num_rows = entries.len().div_ceil(num_columns);

    for row in 0..num_rows {
        let mut line = String::new();
        for col in 0..num_columns {
            let idx = col * num_rows + row;
            if idx < entries.len() {
                let formatted = format_name(&entries[idx], classify);
                line.push_str(&formatted);

                // Pad to column width if not last column
                if col < num_columns - 1 {
                    let visible_len = display_width(&entries[idx], classify);
                    for _ in visible_len..column_width {
                        line.push(' ');
                    }
                }
            }
        }
        writeln!(handle, "{}", line.trim_end())?;
    }

    Ok(())
}

/// Format entries in multi-column layout with horizontal sorting.
fn format_multi_column_horizontal<W: Write>(
    entries: &[ListEntry],
    width: usize,
    classify: bool,
    handle: &mut W,
) -> Result<(), ListError> {
    let max_name_len = entries
        .iter()
        .map(|e| display_width(e, classify))
        .max()
        .unwrap_or(0);

    let column_width = max_name_len + 2;
    let num_columns = std::cmp::max(1, width / column_width);

    for chunk in entries.chunks(num_columns) {
        let mut line = String::new();
        for (i, entry) in chunk.iter().enumerate() {
            let formatted = format_name(entry, classify);
            line.push_str(&formatted);

            // Pad to column width if not last in row
            if i < chunk.len() - 1 {
                let visible_len = display_width(entry, classify);
                for _ in visible_len..column_width {
                    line.push(' ');
                }
            }
        }
        writeln!(handle, "{}", line.trim_end())?;
    }

    Ok(())
}

// =============================================================================
// JSON Format
// =============================================================================

/// Format entries in JSON format (JSONL).
fn format_json(entries: &[ListEntry], include_id: bool) -> Result<(), ListError> {
    let stdout = io::stdout();
    let mut handle = stdout.lock();

    for entry in entries {
        let json_str = match entry {
            ListEntry::File { name, size, executable, id } => {
                let json = ListEntryFileJson {
                    type_tag: "File".to_string(),
                    size: *size,
                    executable: if *executable { Some(true) } else { None },
                    name: name.clone(),
                    id: if include_id { Some(id.to_string()) } else { None },
                };
                serde_json::to_string(&json).unwrap_or_default()
            }
            ListEntry::Directory { name, id } => {
                let json = ListEntryDirectoryJson {
                    type_tag: "Directory".to_string(),
                    name: name.clone(),
                    id: if include_id { Some(id.to_string()) } else { None },
                };
                serde_json::to_string(&json).unwrap_or_default()
            }
        };
        writeln!(handle, "{}", json_str)?;
    }

    Ok(())
}

// =============================================================================
// Formatting Helpers
// =============================================================================

/// Format a name with colors and optional classify suffix.
fn format_name(entry: &ListEntry, classify: bool) -> String {
    let name = shell_escape(entry.name());

    let (colored_name, suffix) = match entry {
        ListEntry::Directory { .. } => {
            let colored = name.with(Color::Blue).to_string();
            let suffix = if classify { "/" } else { "" };
            (colored, suffix)
        }
        ListEntry::File { executable: true, .. } => {
            let colored = name.with(Color::Green).to_string();
            let suffix = if classify { "*" } else { "" };
            (colored, suffix)
        }
        ListEntry::File { executable: false, .. } => {
            // Regular files have no color (default)
            (name, "")
        }
    };

    if suffix.is_empty() {
        colored_name
    } else {
        // Suffix is in default color
        format!("{}{}", colored_name, suffix)
    }
}

/// Calculate the display width of an entry name (excluding ANSI codes).
fn display_width(entry: &ListEntry, classify: bool) -> usize {
    let name_len = shell_escape(entry.name()).len();
    let suffix_len = if classify {
        if entry.is_directory() || entry.is_executable() {
            1
        } else {
            0
        }
    } else {
        0
    };
    name_len + suffix_len
}

/// Shell-escape a filename for display.
///
/// Escapes special characters that would need quoting in a shell.
fn shell_escape(s: &str) -> String {
    let needs_escape = s.chars().any(|c| {
        matches!(c, ' ' | '\t' | '\n' | '\r' | '\\' | '\'' | '"' | '`' | '$' | '!' | '*' | '?' | '[' | ']' | '{' | '}' | '|' | '&' | ';' | '<' | '>' | '(' | ')' | '#' | '~')
    });

    if !needs_escape {
        return s.to_string();
    }

    // Use single quotes and escape single quotes within
    let mut result = String::with_capacity(s.len() + 2);
    result.push('\'');
    for c in s.chars() {
        if c == '\'' {
            result.push_str("'\\''");
        } else {
            result.push(c);
        }
    }
    result.push('\'');
    result
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shell_escape_simple() {
        assert_eq!(shell_escape("hello"), "hello");
        assert_eq!(shell_escape("hello.txt"), "hello.txt");
    }

    #[test]
    fn test_shell_escape_spaces() {
        assert_eq!(shell_escape("hello world"), "'hello world'");
    }

    #[test]
    fn test_shell_escape_quotes() {
        assert_eq!(shell_escape("it's"), "'it'\\''s'");
    }

    #[test]
    fn test_shell_escape_special_chars() {
        assert_eq!(shell_escape("file$name"), "'file$name'");
        assert_eq!(shell_escape("file*"), "'file*'");
    }

    #[test]
    fn test_display_width() {
        let file_entry = ListEntry::File {
            name: "test.txt".to_string(),
            size: 100,
            executable: false,
            id: "abc123".to_string(),
        };
        assert_eq!(display_width(&file_entry, false), 8);
        assert_eq!(display_width(&file_entry, true), 8); // no suffix for regular file

        let exec_entry = ListEntry::File {
            name: "script".to_string(),
            size: 100,
            executable: true,
            id: "abc123".to_string(),
        };
        assert_eq!(display_width(&exec_entry, false), 6);
        assert_eq!(display_width(&exec_entry, true), 7); // + "*"

        let dir_entry = ListEntry::Directory {
            name: "mydir".to_string(),
            id: "abc123".to_string(),
        };
        assert_eq!(display_width(&dir_entry, false), 5);
        assert_eq!(display_width(&dir_entry, true), 6); // + "/"
    }
}
