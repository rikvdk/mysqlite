use std::error::Error;
use std::fmt::{self, Display};
use std::fs::OpenOptions;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::str::FromStr;

enum Statement {
    Insert(Row),
    Select,
}

enum PrepareResult {
    SyntaxError,
    StringTooLong,
    UnrecognizedStatement,
}

enum MetaCommandResult {
    UnrecognizedCommand,
}

enum RunControl {
    Exit,
}

struct Row {
    id: u32,
    username: [u8; Self::USERNAME_SIZE],
    email: [u8; Self::EMAIL_SIZE],
}

impl Row {
    const ID_SIZE: usize = std::mem::size_of::<u32>();
    const USERNAME_SIZE: usize = 32;
    const EMAIL_SIZE: usize = 255;
    const SIZE: usize = Self::ID_SIZE + Self::USERNAME_SIZE + Self::EMAIL_SIZE;

    fn username_str(&self) -> &str {
        Self::bytes_to_str(&self.username)
    }

    fn email_str(&self) -> &str {
        Self::bytes_to_str(&self.email)
    }

    fn bytes_to_str(bytes: &[u8]) -> &str {
        bytes
            .split(|&b| b == 0)
            .next()
            .and_then(|s| std::str::from_utf8(s).ok())
            .unwrap_or("<Invalid utf-8>")
    }
}

impl FromStr for Row {
    type Err = PrepareResult;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut parts = s.split_whitespace();
        let id = parts
            .next()
            .ok_or(PrepareResult::SyntaxError)?
            .parse()
            .map_err(|_| PrepareResult::SyntaxError)?;

        let username = parts.next().ok_or(PrepareResult::SyntaxError)?.as_bytes();
        if username.len() > Self::USERNAME_SIZE {
            return Err(PrepareResult::StringTooLong);
        }

        let email = parts.next().ok_or(PrepareResult::SyntaxError)?.as_bytes();
        if email.len() > Self::EMAIL_SIZE {
            return Err(PrepareResult::StringTooLong);
        }

        let mut row = Self {
            id,
            username: [0; Self::USERNAME_SIZE],
            email: [0; Self::EMAIL_SIZE],
        };

        row.username[..username.len()].copy_from_slice(username);
        row.email[..email.len()].copy_from_slice(email);

        Ok(row)
    }
}

impl Display for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "({} {} {})",
            self.id,
            self.username_str(),
            self.email_str()
        )
    }
}

struct Pager {
    file: std::fs::File,
    pages: Vec<Option<Box<[u8; Pager::SIZE]>>>,
}

impl Pager {
    const SIZE: usize = 4096;

    fn new(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o0600)
            .open(path)?;

        let file_length = file.metadata()?.len();
        let page_count = usize::try_from(file_length.div_ceil(Self::SIZE as u64))?;

        Ok(Self {
            file,
            pages: vec![None; page_count],
        })
    }

    fn get_page(&mut self, page_num: usize) -> Result<&mut [u8; Self::SIZE], Box<dyn Error>> {
        if page_num >= self.pages.len() {
            self.pages.resize(page_num + 1, None);
        }

        if self.pages[page_num].is_none() {
            let mut page = Box::new([0u8; Self::SIZE]);

            let file_length = self.file.metadata()?.len();
            let num_pages = file_length.div_ceil(Self::SIZE as u64);

            if (page_num as u64) < num_pages {
                let offset = page_num as u64 * Self::SIZE as u64;
                let bytes_to_read = usize::try_from(std::cmp::min(
                    Self::SIZE as u64,
                    file_length.saturating_sub(offset),
                ))?;

                self.file.read_exact(&mut page[..bytes_to_read])?;
            }

            self.pages[page_num] = Some(page);
        }

        let page = self.pages[page_num]
            .as_deref_mut()
            .expect("page must be initialized before returning");

        Ok(page)
    }

    fn flush_page(&mut self, index: usize, size: usize) -> io::Result<()> {
        let Some(page) = &self.pages[index] else {
            return Ok(());
        };

        let offset = (index as u64) * (Self::SIZE as u64);

        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(&page[..size])
    }
}

struct Table {
    row_count: usize,
    pager: Pager,
}

impl Table {
    const ROWS_PER_PAGE: usize = Pager::SIZE / Row::SIZE;

    fn new(path: impl AsRef<Path>) -> Result<Self, Box<dyn Error>> {
        let pager = Pager::new(path)?;
        let file_length = usize::try_from(pager.file.metadata()?.len())?;
        let row_count = file_length / Row::SIZE;

        Ok(Self { row_count, pager })
    }

    fn close(&mut self) -> io::Result<()> {
        let full_page_count = self.row_count / Self::ROWS_PER_PAGE;
        for i in 0..full_page_count {
            if self.pager.pages[i].is_some() {
                self.pager.flush_page(i, Pager::SIZE)?;
            }
        }

        let additional_row_count = self.row_count % Self::ROWS_PER_PAGE;
        if additional_row_count > 0 {
            self.pager
                .flush_page(full_page_count, additional_row_count * Row::SIZE)?;
        }

        self.pager.file.sync_all()
    }

    fn insert(&mut self, row: &Row) -> Result<(), Box<dyn Error>> {
        let page_num = self.row_count / Self::ROWS_PER_PAGE;

        let row_offset = self.row_count % Self::ROWS_PER_PAGE;
        let byte_offset = row_offset * Row::SIZE;

        let page = self.pager.get_page(page_num)?;

        page[byte_offset..byte_offset + Row::ID_SIZE].copy_from_slice(&row.id.to_le_bytes());

        let username_offset = byte_offset + Row::ID_SIZE;
        page[username_offset..username_offset + Row::USERNAME_SIZE].copy_from_slice(&row.username);

        let email_offset = username_offset + Row::USERNAME_SIZE;
        page[email_offset..email_offset + Row::EMAIL_SIZE].copy_from_slice(&row.email);

        self.row_count += 1;

        Ok(())
    }

    fn select<W>(&mut self, output: &mut W) -> Result<(), Box<dyn Error>>
    where
        W: io::Write,
    {
        for i in 0..self.row_count {
            let row = self.deserialize_row(i)?;
            writeln!(output, "{row}")?;
        }

        Ok(())
    }

    fn deserialize_row(&mut self, index: usize) -> Result<Row, Box<dyn Error>> {
        let page_num = index / Self::ROWS_PER_PAGE;
        let row_offset = index % Self::ROWS_PER_PAGE;
        let byte_offset = row_offset * Row::SIZE;

        let page = self.pager.get_page(page_num)?;
        let id = u32::from_le_bytes(page[byte_offset..byte_offset + Row::ID_SIZE].try_into()?);

        let mut username = [0; Row::USERNAME_SIZE];
        let mut email = [0; Row::EMAIL_SIZE];

        let username_offset = byte_offset + Row::ID_SIZE;
        username.copy_from_slice(&page[username_offset..username_offset + Row::USERNAME_SIZE]);

        let email_offset = username_offset + Row::USERNAME_SIZE;
        email.copy_from_slice(&page[email_offset..email_offset + Row::EMAIL_SIZE]);

        Ok(Row {
            id,
            username,
            email,
        })
    }
}

fn prepare_statement(input_buffer: &str) -> Result<Statement, PrepareResult> {
    if let Some(stripped) = input_buffer.strip_prefix("insert") {
        let row = Row::from_str(stripped)?;
        Ok(Statement::Insert(row))
    } else if input_buffer.starts_with("select") {
        Ok(Statement::Select)
    } else {
        Err(PrepareResult::UnrecognizedStatement)
    }
}

fn execute_statement<W>(
    statement: &Statement,
    table: &mut Table,
    output: &mut W,
) -> Result<(), Box<dyn Error>>
where
    W: io::Write,
{
    match statement {
        Statement::Insert(row) => {
            table.insert(row)?;
            Ok(())
        }
        Statement::Select => table.select(output),
    }
}

fn print_prompt<W>(output: &mut W) -> io::Result<()>
where
    W: io::Write,
{
    write!(output, "mysqlite> ")?;
    output.flush()
}

fn read_input<'a, R>(input: &mut R, input_buffer: &'a mut String) -> Result<&'a str, io::Error>
where
    R: io::BufRead,
{
    input_buffer.clear();
    input.read_line(input_buffer)?;
    Ok(input_buffer.trim())
}

fn do_meta_command(command: &str) -> Result<RunControl, MetaCommandResult> {
    match command {
        ".exit" => Ok(RunControl::Exit),
        _ => Err(MetaCommandResult::UnrecognizedCommand),
    }
}

fn run<R, W>(input: &mut R, output: &mut W, path: impl AsRef<Path>) -> Result<(), Box<dyn Error>>
where
    R: io::BufRead,
    W: io::Write,
{
    let mut table = Table::new(path)?;
    let mut input_buffer = String::new();

    loop {
        print_prompt(output)?;

        let command = read_input(input, &mut input_buffer)?;

        if command.is_empty() {
            continue;
        }

        if command.starts_with('.') {
            match do_meta_command(command) {
                Ok(RunControl::Exit) => {
                    table.close()?;
                    return Ok(());
                }
                Err(MetaCommandResult::UnrecognizedCommand) => {
                    writeln!(output, "Unrecognized command '{command}'")?;
                }
            }
            continue;
        }

        let statement = match prepare_statement(command) {
            Ok(statement) => statement,
            Err(err) => {
                match err {
                    PrepareResult::SyntaxError => {
                        writeln!(output, "Syntax error. Could not parse statement.")?;
                    }
                    PrepareResult::StringTooLong => writeln!(output, "String is too long.")?,
                    PrepareResult::UnrecognizedStatement => {
                        writeln!(output, "Unrecognized keyword at start of '{command}'.")?;
                    }
                }
                continue;
            }
        };

        execute_statement(&statement, &mut table, output)?;
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let mut stdin = io::stdin().lock();
    let mut stdout = io::stdout().lock();
    run(&mut stdin, &mut stdout, "mysqlite.db")
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use tempfile::TempDir;

    use super::{Error, io, run};

    #[test]
    fn test_simple_insert_and_select() {
        let scripts = ["insert 1 user1 person1@example.com", "select", ".exit"];
        let (_dir, path) = create_test_db_file();
        let output = run_scripts(&scripts, &path).unwrap();

        assert_eq!(
            output,
            "mysqlite> mysqlite> (1 user1 person1@example.com)\nmysqlite> "
        );
    }

    #[test]
    fn test_username_max_length() {
        let scripts = [
            "insert 1 abcdefghijklmnopqrstuvwxyzabcdef a@b.com",
            "select",
            ".exit",
        ];
        let (_dir, path) = create_test_db_file();
        let output = run_scripts(&scripts, &path).unwrap();

        assert_eq!(
            output,
            "mysqlite> mysqlite> (1 abcdefghijklmnopqrstuvwxyzabcdef a@b.com)\nmysqlite> "
        );
    }
    #[test]
    fn test_username_too_long() {
        let scripts = [
            "insert 1 abcdefghijklmnopqrstuvwxyzabcdefg a@b.com",
            ".exit",
        ];
        let (_dir, path) = create_test_db_file();
        let output = run_scripts(&scripts, &path).unwrap();

        assert_eq!(output, "mysqlite> String is too long.\nmysqlite> ");
    }

    #[test]
    fn test_email_max_length() {
        let n = 255;
        let insert_str = &format!("insert 1 u {0:a<1$}", "", n);
        let scripts = [insert_str, ".exit"];
        let (_dir, path) = create_test_db_file();
        let output = run_scripts(&scripts, &path).unwrap();

        assert_eq!(output, "mysqlite> mysqlite> ");
    }

    #[test]
    fn test_email_too_long() {
        let n = 256;
        let insert_str = &format!("insert 1 u {0:a<1$}", "", n);
        let scripts = [insert_str, ".exit"];
        let (_dir, path) = create_test_db_file();
        let output = run_scripts(&scripts, &path).unwrap();

        assert_eq!(output, "mysqlite> String is too long.\nmysqlite> ");
    }

    #[test]
    fn test_persistent_data() {
        let scripts = ["insert 1 user1 person1@example.com", ".exit"];
        let (_dir, path) = create_test_db_file();
        let output = run_scripts(&scripts, &path).unwrap();
        assert_eq!(output, "mysqlite> mysqlite> ");

        let scripts = ["select", ".exit"];
        let output = run_scripts(&scripts, &path).unwrap();
        assert_eq!(
            output,
            "mysqlite> (1 user1 person1@example.com)\nmysqlite> "
        );
    }

    fn run_scripts(commands: &[&str], path: &impl AsRef<Path>) -> Result<String, Box<dyn Error>> {
        let input = String::from(commands.join("\n"));
        let mut input = io::Cursor::new(&input[..]);
        let mut output = vec![];

        run(&mut input, &mut output, path)?;

        Ok(std::str::from_utf8(&output)?.into())
    }

    fn create_test_db_file() -> (TempDir, PathBuf) {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("test.db");
        return (dir, path);
    }
}
