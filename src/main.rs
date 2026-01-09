use std::error::Error;
use std::fmt::{self, Display};
use std::io;
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

#[derive(Default)]
struct Table {
    row_count: usize,
    pages: Vec<[u8; Self::PAGE_SIZE]>,
}

impl Table {
    const PAGE_SIZE: usize = 4096;
    const ROWS_PER_PAGE: usize = Self::PAGE_SIZE / Row::SIZE;

    fn insert(&mut self, row: &Row) {
        let page_num = self.row_count / Self::ROWS_PER_PAGE;

        if self.pages.len() <= page_num {
            self.pages.push([0u8; Self::PAGE_SIZE]);
        }

        let row_offset = self.row_count % Self::ROWS_PER_PAGE;
        let byte_offset = row_offset * Row::SIZE;

        let page = &mut self.pages[page_num];

        page[byte_offset..byte_offset + Row::ID_SIZE].copy_from_slice(&row.id.to_le_bytes());

        let username_offset = byte_offset + Row::ID_SIZE;
        page[username_offset..username_offset + Row::USERNAME_SIZE].copy_from_slice(&row.username);

        let email_offset = username_offset + Row::USERNAME_SIZE;
        page[email_offset..email_offset + Row::EMAIL_SIZE].copy_from_slice(&row.email);

        self.row_count += 1;
    }

    fn select<W>(&self, output: &mut W) -> Result<(), Box<dyn Error>>
    where
        W: io::Write,
    {
        for i in 0..self.row_count {
            let row = self.deserialize_row(i);
            writeln!(output, "{row}")?;
        }

        Ok(())
    }

    fn deserialize_row(&self, index: usize) -> Row {
        let page_num = index / Self::ROWS_PER_PAGE;
        let row_offset = index % Self::ROWS_PER_PAGE;
        let byte_offset = row_offset * Row::SIZE;

        let page = &self.pages[page_num];
        let id = u32::from_le_bytes(
            page[byte_offset..byte_offset + Row::ID_SIZE]
                .try_into()
                .unwrap(),
        );

        let mut username = [0; Row::USERNAME_SIZE];
        let mut email = [0; Row::EMAIL_SIZE];

        let username_offset = byte_offset + Row::ID_SIZE;
        username.copy_from_slice(&page[username_offset..username_offset + Row::USERNAME_SIZE]);

        let email_offset = username_offset + Row::USERNAME_SIZE;
        email.copy_from_slice(&page[email_offset..email_offset + Row::EMAIL_SIZE]);

        Row {
            id,
            username,
            email,
        }
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
            table.insert(row);
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

fn run<R, W>(input: &mut R, output: &mut W) -> Result<(), Box<dyn Error>>
where
    R: io::BufRead,
    W: io::Write,
{
    let mut table = Table::default();
    let mut input_buffer = String::new();

    loop {
        print_prompt(output)?;

        let command = read_input(input, &mut input_buffer)?;

        if command.is_empty() {
            continue;
        }

        if command.starts_with('.') {
            match do_meta_command(command) {
                Ok(RunControl::Exit) => return Ok(()),
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
    run(&mut stdin, &mut stdout)
}

#[cfg(test)]
mod tests {
    use super::{Error, io, run};

    #[test]
    fn test_simple_insert_and_select() {
        let scripts = ["insert 1 user1 person1@example.com", "select", ".exit"];
        let output = run_scripts(&scripts).unwrap();
        let output = std::str::from_utf8(&output).unwrap();

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
        let output = run_scripts(&scripts).unwrap();
        let output = std::str::from_utf8(&output).unwrap();

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
        let output = run_scripts(&scripts).unwrap();
        let output = std::str::from_utf8(&output).unwrap();

        assert_eq!(output, "mysqlite> String is too long.\nmysqlite> ",);
    }

    #[test]
    fn test_email_max_length() {
        let n = 255;
        let insert_str = &format!("insert 1 u {0:a<1$}", "", n);
        let scripts = [insert_str, ".exit"];
        let output = run_scripts(&scripts).unwrap();
        let output = std::str::from_utf8(&output).unwrap();

        assert_eq!(output, "mysqlite> mysqlite> ",);
    }

    #[test]
    fn test_email_too_long() {
        let n = 256;
        let insert_str = &format!("insert 1 u {0:a<1$}", "", n);
        let scripts = [insert_str, ".exit"];
        let output = run_scripts(&scripts).unwrap();
        let output = std::str::from_utf8(&output).unwrap();

        assert_eq!(output, "mysqlite> String is too long.\nmysqlite> ",);
    }

    fn run_scripts(commands: &[&str]) -> Result<Vec<u8>, Box<dyn Error>> {
        let input = String::from(commands.join("\n"));
        let mut input = io::Cursor::new(&input[..]);
        let mut output = vec![];

        run(&mut input, &mut output)?;

        Ok(output)
    }
}
